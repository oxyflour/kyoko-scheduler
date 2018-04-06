import * as os from 'os'
import KyokoMesh from 'kyoko-mesh'
import { EventEmitter } from 'events'
import { spawn } from 'child_process'
import { Etcd3, Namespace, IOptions, Lease } from 'etcd3'

import schedulerAPI from './api/scheduler'
import workerAPI from './api/worker'
import executorAPI from './api/executor'
import { Job, Worker, Task, Step } from './models'
import { Usage, debounce } from './utils'

const DEFAULT_CONFIG = {
    id: `${Math.random().toString(16).slice(2, 10)}@${os.hostname()}`,
    logger: console,
    etcdOpts: {
        hosts: 'http://localhost:2379'
    } as IOptions,
    etcdPrefix: 'kyoko-scheduler/' as string,
    etcdLease: 10,
    pollInterval: 5,
    startScheduler: false,
    startWatcher: false,
    startWorker: false,
    startExecutor: null as null | Partial<Task>,
}

export default class Service extends EventEmitter {
    private readonly etcd: Namespace
    private readonly opts: typeof DEFAULT_CONFIG
    constructor(opts = { } as Partial<typeof DEFAULT_CONFIG>) {
        super()
        this.opts = { ...DEFAULT_CONFIG, ...opts }
        this.etcd = new Etcd3(this.opts.etcdOpts).namespace(this.opts.etcdPrefix)
        this.init().catch(err => this.opts.logger.error(err))
    }

    private async init() {
        const { id, etcdLease, etcdOpts, etcdPrefix } = this.opts,
            mesh = new KyokoMesh({ etcdLease, etcdOpts, etcdPrefix, nodeName: id }),
            opts = { etcd: this.etcd, logger: this.opts.logger, id: this.opts.id },
            lease = this.etcd.lease(this.opts.etcdLease)
        if (this.opts.startScheduler) {
            this.opts.logger.log(`starting node "${this.opts.id}" as scheduler`)
            mesh.register(schedulerAPI({ ...opts, mesh }))
        }
        if (this.opts.startWatcher) {
            this.opts.logger.log(`starting node "${this.opts.id}" as watcher`)
            await this.initWatcher(mesh)
        }
        if (this.opts.startWorker) {
            this.opts.logger.log(`starting node "${this.opts.id}" as worker`)
            await this.initWorker(lease)
            mesh.register(workerAPI({ ...opts }))
        }
        if (this.opts.startExecutor) {
            this.opts.logger.log(`starting node "${this.opts.id}" as executor`)
            const { proc } = await this.initExecutor(lease, this.opts.startExecutor || { })
            mesh.register(executorAPI({ ...opts, proc }))
        }

        await mesh.init()
        this.emit('ready', mesh)
        if (this.opts.startScheduler || this.opts.startWorker || this.opts.startExecutor) {
            this.opts.logger.log(`node "${this.opts.id}" ready`)
        }

        while (1) {
            try {
                if (this.opts.startExecutor) {
                    await this.updateExecutor(lease)
                }
                if (this.opts.startWorker) {
                    await this.updateWorker(lease)
                }
            } catch (err) {
                this.opts.logger.error(err)
            }
            await new Promise(resolve => setTimeout(resolve, this.opts.pollInterval * 1000))
        }
    }

    private async initWorker(lease: Lease) {
        const { id } = this.opts
        {
            const namespace = this.etcd.namespace(`worker/${id}/tags/`),
                watcher = await namespace.watch().prefix('').create(),
                tags = await namespace.getAll().keys()
            await lease.put(`worker/tagged/any/${id}`).value(1)
            await Promise.all(tags.map(tag => lease.put(`worker/tagged/${tag}/${id}`).value(1)))
            watcher.on('put', kv => lease.put(`worker/tagged/${kv.key.toString()}/${id}`).value(1))
            watcher.on('delete', kv => this.etcd.delete().key(`worker/tagged/${kv.key.toString()}/${id}`))
        }
        {
            const namespace = this.etcd.namespace(`task/running/${id}/`),
                watcher = await namespace.watch().prefix('').create(),
                update = debounce(() => this.updateWorker(lease).catch(err => this.opts.logger.error('update', err)), 1000)
            watcher.on('connected', update)
            watcher.on('put',       update)
            watcher.on('delete',    update)
        }
        await this.updateWorker(lease)
    }
    private async updateWorker(lease: Lease) {
        const { id } = this.opts,
            running = await this.etcd.namespace(`task/running/${id}/`).getAll().json(),
            total = {
                cpu: [os.cpus().length],
                mem: [os.totalmem()],
                // TODO: add more resource
            },
            usage = new Usage().add(...Object.values(running).map(task => new Task(task).usage)).toPlain(),
            meta = { id, total, usage } as Partial<Worker>
        await lease.put(`worker/${this.opts.id}`).value(JSON.stringify(meta))
        return meta
    }

    private async initExecutor(lease: Lease, task: Partial<Task>) {
        const { id } = this.opts,
            cmd = task.cmd || `echo "no cmd defined"`,
            cwd = task.cwd || '/',
            env = task.env || { },
            shell = true,
            stdio = 'inherit',
            proc = spawn(cmd, [ ], { stdio, env, cwd, shell })
        proc.on('exit', async (code, signal) => {
            this.opts.logger.log(`exited with code ${code}, signal ${signal}`)
            const task = await this.updateExecutor(lease, { finished: Date.now(), code })
            await this.etcd.put(`step/started/${task.job}/${task.step}/${id}`).value(JSON.stringify(task))
            await lease.revoke()
            process.exit(code)
        })
        if (process.send) {
            process.send({ startedTaskId: this.opts.id })
        }
        await this.updateExecutor(lease, { created: Date.now() })
        return { proc }
    }
    private async updateExecutor(lease: Lease, update = { } as Partial<Task>) {
        const { id } = this.opts,
            task = new Task(this.opts.startExecutor || { }),
            job = new Job(await this.etcd.get(`job/submited/${task.job}`).json()),
            step = new Step(job.steps[task.step]),
            usage = await step.usage(task, job),
            meta = Object.assign(this.opts.startExecutor, { usage }, update)
        const ticks = meta.finished &&
            Object.keys(task.usage).filter(time => parseInt(time) + (task.created || 0) > (meta.finished || 0))
        if (meta.finished && ticks && ticks.length) {
            meta.usage[meta.finished] = new Usage(meta.usage).at(meta.finished)
            ticks.forEach(tick => delete meta.usage[tick])
        }
        const val = JSON.stringify(meta)
        await lease.put(`task/running/${task.worker}/${id}`).value(val)
        await lease.put(`task/executing/${task.job}/${id}`).value(val)
        return meta
    }

    private async initWatcher(mesh: KyokoMesh) {
        const api = mesh.query(schedulerAPI({ } as any)).scheduler,
            check = debounce(() => api.check().catch(err => this.opts.logger.error('check', err)), 1000)

        const jobWatcher = await this.etcd.namespace(`job/submited/`).watch().prefix('').create()
        jobWatcher.on('put',    check)
        jobWatcher.on('delete', check)

        const stepWatcher = await this.etcd.namespace(`step/done/`).watch().prefix('').create()
        stepWatcher.on('put',   check)

        const taskWatcher = await this.etcd.namespace(`task/running/`).watch().prefix('').create()
        taskWatcher.on('delete', check)
    }
}
