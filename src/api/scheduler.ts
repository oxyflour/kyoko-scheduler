import KyokoMesh from 'kyoko-mesh'
import { Namespace } from 'etcd3'

import workerAPI from './worker'
import { Job, Step, Task, Worker, Dict, IResource, Resource, Usage } from '../models'
import { arrMap, mapMap } from '../utils'

export interface ApiOpts {
    id: string,
    logger: typeof console
    etcd: Namespace
    mesh: KyokoMesh
}

const WORKER_RPC = workerAPI({ } as any)
export const api = ({ etcd, mesh, logger }: ApiOpts) => ({
    async submit(job: Partial<Job>) {
        const id = job.id = job.id || Math.random().toString(16).slice(2, 10)
        await etcd.put(`submited/${id}`).value(JSON.stringify(job))
        logger.log(`submited job "${id}"`, job)
        return id
    },
    async test(worker: Partial<Worker>, usage: Partial<Usage>) {
        const used = await etcd.namespace(`used/${worker.id}/`).getAll().json(),
            usages = Object.values(used).concat(usage).map(usd => new Usage(usd)),
            output = { } as { [time: string]: Resource }
        for (const usage of usages) {
            for (const time in usage.res || { }) {
                const tick = (usage.task && usage.task.created || 0) + parseInt(time)
                output[tick] = output[tick] || new Resource(worker.resource)
            }
        }
        for (const tick of Object.keys(output).map(tick => parseInt(tick))) {
            for (const res of usages.map(usage => usage.getAt(tick)).filter(r => r)) {
                output[tick] = output[tick].sub(res as any as IResource)
            }
        }
        return Object.values(output).every(res => res.valid())
    },
    async query(tags: string[], usage: Partial<Usage>) {
        const [head, ...rest] = await Promise
                .all(tags.map(tag => etcd.namespace(`tagged/${tag}/`).getAll().json())),
            workers = Object.keys(head || { })
                .filter(id => rest.every(dict => !!dict[id]))
                .map(id => new Worker(head[id])),
            capability = await Promise.all(workers.map(worker => this.test(worker, usage))),
            avail = workers.filter((_, index) => capability[index])
        logger.log(`got ${workers.length} workers for tags ${tags}, ${avail.length} available`)
        return avail
    },
    async dispatch(job: Partial<Job>, step: string, worker: Partial<Worker>, tasks: Dict<Partial<Task>>) {
        logger.log(`dispatching job "${job.id}", step "${step}" to worker "${worker.id}"`, tasks)
        const started = await mesh.query(WORKER_RPC).workers[worker.id || ''].start(tasks)
        await Promise.all(started.map(id =>
            etcd.put(`job/${job.id}/started/${step}/${id}`).value(JSON.stringify(tasks[id]))))
    },
    async start(j: Partial<Job>, step: string, deps: Dict<Dict<Partial<Task>>>) {
        const job = new Job(j),
            registry = await etcd.namespace(`job/${job.id}/started/${step}/`).getAll().json(),
            started = mapMap(registry, data => new Task(data)),
            stp = new Step(job.steps[step]),
            usage = await stp.require(job, step, deps, started),
            workers = await this.query(stp.tags, usage),
            plans = await stp.plan(job, step, deps, started, workers)
        logger.log(`job "${job.id}" got ${workers.length} workers, ${plans ? plans.length : 'no'} plans`)
        if (plans) {
            await Promise.all(plans.map(({ worker, tasks }) => this.dispatch(job, step, worker, tasks)))
        } else {
            await etcd.put(`job/${job.id}/success/${step}`).value(JSON.stringify(registry))
            logger.log(`job "${job.id}", step "${step}" finished`)
        }
    },
    async update(id: string) {
        const job = new Job(await etcd.get(`submited/${id}`).json()),
            success = await etcd.namespace(`job/${job.id}/success/`).getAll().json(),
            steps = Object.keys(job.steps).filter(step => !success[step])
        logger.log(`updating job "${id}", steps to run: ${steps.length ? steps : 'none'}`)
        if (steps.length) {
            await Promise.all(steps
                .map(step => ({ step, deps: arrMap(job.deps(step), dep =>
                    mapMap(success[dep] as any, data => new Task(data))) }))
                .map(({ step, deps }) => this.start(job, step, deps)))
        } else {
            await etcd.delete().key(`submited/${id}`).exec()
            logger.log(`job "${id}" done`)
        }
    },
    async check() {
        const jobs = await etcd.namespace(`submited/`).getAll().keys()
        logger.log(`got ${jobs.length} jobs to update`)
        for (const id of jobs) {
            const lock = await etcd.lock(`update/${id}`).acquire()
            try {
                await this.update(id)
                await lock.release()
            } catch (err) {
                await lock.release()
            }
        }
    },
})

export default (opts: ApiOpts) => ({
    scheduler: api(opts),
})
