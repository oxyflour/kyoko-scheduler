import * as path from 'path'
import * as fs from 'fs'
import * as os from 'os'
import { spawn, ChildProcess } from 'child_process'
import { Namespace } from 'etcd3'

import { Task } from '../models'

export interface ApiOpts {
    id: string
    etcd: Namespace
    logger: typeof console
    forkTimeout: number,
}

function waitForResp(proc: ChildProcess, id: string, timeout: number) {
    return new Promise((resolve, reject) => {
        proc.once('message', () => resolve())
        proc.once('exit', () => reject(Error(`task "${id}" exited without responding message`)))
        setTimeout(reject, timeout, Error(`wait for task "${id}" timeout`))
    })
}

const api = ({ logger, forkTimeout }: ApiOpts) => ({
    async fork(id: string, task: Partial<Task>) {
        const script = path.join(__dirname, '..', '..', 'cli.js'),
            tmpdir = os.tmpdir(),
            tmpfile = path.join(tmpdir, id.replace(/\W+/g, '-')),
            stdout = fs.openSync(`${tmpfile}.stdout`, 'w'),
            stderr = fs.openSync(`${tmpfile}.stderr`, 'w'),
            stdio = ['ignore', stdout, stderr, 'ipc'],
            detached = true,
            proc = spawn(process.execPath, [script, 'execute', id, JSON.stringify(task)], { stdio, detached })
        logger.log(`starting "${process.execPath}", log "${tmpfile}"`, [script, 'execute', id, JSON.stringify(task)])
        try {
            await waitForResp(proc, id, forkTimeout)
            proc.unref()
            logger.log(`fork task "${id}" ok`)
        } catch (err) {
            proc.unref()
            logger.log(`fork task "${id}" failed`, err)
            throw err
        }
    },
    async start(tasks: { [id: string]: Partial<Task> }) {
        logger.log(`tring to start tasks`, tasks)
        const started = [ ] as string[]
        await Promise.all(Object.keys(tasks).map(async id => {
            try {
                this.fork(id, tasks[id])
                started.push(id)
            } catch (err) {
                logger.error(err)
            }
        }))
        return started
    },
})

export default (opts: ApiOpts) => ({
    workers: {
        [opts.id]: api(opts)
    }
})
