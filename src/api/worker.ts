import * as path from 'path'
import * as fs from 'fs'
import * as os from 'os'
import { spawn, ChildProcess } from 'child_process'
import { Namespace } from 'etcd3'

import { Task, Worker } from '../models'

export const WORKER_CONFIG = {
    forkTimeout: 30 * 30 * 1000,
}

export interface ApiOpts {
    id: string
    etcd: Namespace
    logger: typeof console
}

function waitForResp(proc: ChildProcess, id: string, timeout: number) {
    return new Promise((resolve, reject) => {
        proc.on('message', function resp(msg) {
            if (msg && msg.startedTaskId === id) {
                resolve()
                clearTimeout(defered)
                proc.removeListener('message', resp)
            }
        })
        proc.once('exit', () => {
            clearTimeout(defered)
            reject(Error(`task "${id}" exited without responding message`))
        })
        const defered = setTimeout(() => {
            reject(Error(`wait for task "${id}" timeout`))
        }, timeout)
    })
}

const api = ({ etcd, logger, id }: ApiOpts) => ({
    async fork(id: string, task: Partial<Task>, timeout: number) {
        const script = path.join(__dirname, '..', '..', 'cli.js'),
            tmpdir = os.tmpdir(),
            tmpfile = path.join(tmpdir, id.replace(/\W+/g, '-')),
            stdout = fs.openSync(`${tmpfile}.stdout`, 'w'),
            stderr = fs.openSync(`${tmpfile}.stderr`, 'w'),
            stdio = ['ignore', stdout, stderr, 'ipc'],
            detached = true,
            proc = spawn(process.execPath, [script, 'execute', id, JSON.stringify(task)], { stdio, detached })
        logger.log(`starting "${process.execPath}", log "${tmpfile}"`)
        try {
            await waitForResp(proc, id, timeout)
            proc.unref()
            logger.log(`fork task "${id}" ok`)
        } catch (err) {
            proc.unref()
            logger.log(`fork task "${id}" failed`, err)
            throw err
        }
    },
    async start(tasks: { [id: string]: Partial<Task> }) {
        const worker = await etcd.get(`worker/${id}`).json() as Partial<Worker>
        if (!new Worker(worker).canRun(...Object.values(tasks))) {
            throw Error(`no enough resource for tasks ${Object.keys(tasks)}`)
        }
        logger.log(`trying to start tasks ${Object.keys(tasks)}`)
        const started = [ ] as string[],
            config = { ...WORKER_CONFIG, ...await etcd.get(`config/worker`).json() }
        await Promise.all(Object.keys(tasks).map(async id => {
            try {
                await this.fork(id, tasks[id], config.forkTimeout || 5 * 30 * 1000)
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
