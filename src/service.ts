import * as os from 'os'
import KyokoMesh from 'kyoko-mesh'
import { EventEmitter } from 'events'
import { spawn } from 'child_process'
import { Etcd3, Namespace, IOptions, Lease } from 'etcd3'

import schedulerAPI from './api/scheduler'
import workerAPI from './api/worker'
import executorAPI from './api/executor'
import { Worker, Task, Usage } from './models'

const DEFAULT_CONFIG = {
    id: 'N' + Math.random().toString(16).slice(2, 10) + '-H' + os.hostname(),
    logger: console,
    etcdOpts: {
        hosts: 'http://localhost:2379'
    } as IOptions,
    etcdPrefix: 'kyoko-scheduler/' as string,
    etcdLease: 10,
    pollInterval: 5,
    scheduler: { } as Partial<{
        start: boolean,
    }>,
    worker: { } as Partial<{
        start: boolean,
        forkTimeout: number,
    }>,
    executor: { } as Partial<{
        start: boolean,
        task: Partial<Task>,
    }>,
}

export default class Service extends EventEmitter {
    private readonly etcd: Namespace
    private readonly opts: typeof DEFAULT_CONFIG
    constructor(opts = { } as Partial<typeof DEFAULT_CONFIG>) {
        super()
        this.opts = { ...DEFAULT_CONFIG, ...opts }
        this.etcd = new Etcd3(this.opts.etcdOpts).namespace(this.opts.etcdPrefix)
        this.init()
    }

    private async init() {
        const mesh = new KyokoMesh(this.opts),
            opts = { etcd: this.etcd, logger: this.opts.logger, id: this.opts.id },
            lease = this.etcd.lease(this.opts.etcdLease)
        if (this.opts.scheduler.start) {
            this.opts.logger.log(`starting node "${this.opts.id}" as scheduler`)
            mesh.register(schedulerAPI({ ...opts, mesh }))
        }
        if (this.opts.worker.start) {
            this.opts.logger.log(`starting node "${this.opts.id}" as worker`)
            await this.initWorker(lease)
            const forkTimeout = this.opts.worker.forkTimeout || 30 * 60 * 1000
            mesh.register(workerAPI({ ...opts, forkTimeout }))
        }
        if (this.opts.executor.start) {
            this.opts.logger.log(`starting node "${this.opts.id}" as executor`)
            const { proc } = await this.initExecutor(lease, this.opts.executor.task || { })
            mesh.register(executorAPI({ ...opts, proc }))
        }

        await mesh.announce()
        await this.poll(lease)
        this.emit('ready', mesh)

        while (1) {
            try {
                this.poll(lease)
            } catch (err) {
                this.opts.logger.error(err)
            }
            await new Promise(resolve => setTimeout(resolve, this.opts.pollInterval * 1000))
        }
    }

    private async getWorkerMeta() {
        return {
            id: this.opts.id,
            updated: Date.now(),
            resource: {
                cpu: os.cpus().length,
                mem: os.totalmem(),
            }
        } as Partial<Worker>
    }

    private async initWorker(lease: Lease) {
        const val = JSON.stringify(await this.getWorkerMeta()),
            namespace = this.etcd.namespace(`worker/${this.opts.id}/tags/`)
        await lease.put(`tagged/none/${this.opts.id}`).value(val)
        const tags = await namespace.getAll().keys(),
            watcher = await namespace.watch().prefix('').create()
        watcher.on('connected', () => tags.map(tag => lease.put(`tagged/${tag}/${this.opts.id}`).value(val)))
        watcher.on('put', kv => lease.put(`tagged/${kv.key.toString()}/${this.opts.id}`).value(val))
        watcher.on('delete', kv => this.etcd.delete().key(`tagged/${kv.key.toString()}/${this.opts.id}`))
    }

    private async initExecutor(lease: Lease, task: Partial<Task>) {
        await this.poll(lease)
        const cmd = task.cmd || `echo "no cmd defined"`,
            cwd = task.cwd || '/',
            env = task.env || { },
            shell = true,
            proc = spawn(cmd, [ ], { env, cwd, shell })
        proc.on('exit', async (code, signal) => {
            this.opts.logger.log(`exited with code ${code}, signal ${signal}`)
            await lease.revoke()
            process.exit(code)
        })
        if (process.send) {
            process.send({ startedTaskId: this.opts.id })
        }
        return { proc }
    }

    private async getExecutorUsage() {
        const task = new Task(this.opts.executor.task),
            last = Date.now() - task.created + 10 * 60 * 1000
        return {
            task,
            res: {
                [0]:    { cpu: 1 },
                [last]: { cpu: 1 },
            },
        } as Partial<Usage>
    }

    private async poll(lease: Lease) {
        if (this.opts.worker.start) {
            await lease.put(`worker/${this.opts.id}`)
                .value(JSON.stringify(await this.getWorkerMeta()))
        }
        if (this.opts.executor.task) {
            const val = JSON.stringify(await this.getExecutorUsage()),
                { worker } = this.opts.executor.task
            await lease.put(`executor/${this.opts.id}`).value(val)
            if (worker) {
                await lease.put(`used/${worker.id}/${this.opts.id}`).value(val)
            }
        }
    }
}
