import * as os from 'os'
import KyokoMesh from 'kyoko-mesh'
import { EventEmitter } from 'events'
import { spawn } from 'child_process'
import { Etcd3, Namespace, IOptions, Lease } from 'etcd3'

import schedulerAPI from './api/scheduler'
import workerAPI from './api/worker'
import executorAPI from './api/executor'
import { Worker, Task, Step } from './models'

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
    watcher: { } as Partial<{
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
        if (this.opts.watcher.start) {
            this.opts.logger.log(`starting node "${this.opts.id}" as watcher`)
            await this.initWatcher(mesh)
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
        if (this.opts.scheduler.start || this.opts.worker.start || this.opts.executor.start) {
            this.opts.logger.log(`node "${this.opts.id}" ready`)
        }

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

    private async getTaskMeta() {
        const task = new Task(this.opts.executor.task),
            step = new Step((task.job.steps || { })[task.step]),
            usage = await step.usage(task),
            updated = Date.now()
        return Object.assign(task, { usage, updated })
    }

    private async initWorker(lease: Lease) {
        const val = JSON.stringify(await this.getWorkerMeta()),
            namespace = this.etcd.namespace(`worker/${this.opts.id}/tags/`)
        await lease.put(`tagged/any/${this.opts.id}`).value(val)
        const tags = await namespace.getAll().keys(),
            watcher = await namespace.watch().prefix('').create()
        watcher.on('connected', () => tags.map(tag => lease.put(`tagged/${tag}/${this.opts.id}`).value(val)))
        watcher.on('put', kv => lease.put(`tagged/${kv.key.toString()}/${this.opts.id}`).value(val))
        watcher.on('delete', kv => this.etcd.delete().key(`tagged/${kv.key.toString()}/${this.opts.id}`))
    }

    private async initExecutor(lease: Lease, task: Partial<Task>) {
        Object.assign(this.opts.executor.task, { created: Date.now() })
        await this.poll(lease)
        const cmd = task.cmd || `echo "no cmd defined"`,
            cwd = task.cwd || '/',
            env = task.env || { },
            stdio = 'inherit',
            shell = true,
            proc = spawn(cmd, [ ], { stdio, env, cwd, shell })
        proc.on('exit', async (code, signal) => {
            this.opts.logger.log(`exited with code ${code}, signal ${signal}`)
            Object.assign(this.opts.executor.task, { finished: Date.now() })
            await this.poll(lease)
            await lease.revoke()
            process.exit(code)
        })
        if (process.send) {
            process.send({ startedTaskId: this.opts.id })
        }
        return { proc }
    }

    watchQueue = Promise.resolve()
    private pushQueue(fn: (...args: any[]) => Promise<any>) {
        this.watchQueue = this.watchQueue.then(fn).catch(err => console.log(err))
    }
    private async initWatcher(mesh: KyokoMesh) {
        const api = mesh.query(schedulerAPI({ } as any)).scheduler

        const jobWatcher = await this.etcd.namespace(`submited/`).watch().prefix('').create()
        jobWatcher.on('put', () => this.pushQueue(api.check))

        const stepWatcher = await this.etcd.namespace(`success/`).watch().prefix('').create()
        stepWatcher.on('put', () => this.pushQueue(api.check))

        const taskWatcher = await this.etcd.namespace(`executor/`).watch().prefix('').create()
        taskWatcher.on('delete', () => this.pushQueue(api.check))
    }

    private async poll(lease: Lease) {
        if (this.opts.worker.start) {
            const meta = JSON.stringify(await this.getWorkerMeta())
            await lease.put(`worker/${this.opts.id}`).value(meta)
        }
        if (this.opts.executor.task) {
            const meta = JSON.stringify(await this.getTaskMeta()),
                { worker, job, step } = this.opts.executor.task
            await lease.put(`executor/${job}/${this.opts.id}`).value(meta)
            await lease.put(`working/${worker}/${this.opts.id}`).value(meta)
            await this.etcd.put(`job/${job}/started/${step}/${this.opts.id}`).value(meta)
        }
    }
}
