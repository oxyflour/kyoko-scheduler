import KyokoMesh from 'kyoko-mesh'
import { Namespace } from 'etcd3'

import workerAPI from './worker'
import executorAPI from './executor'
import { Job, Step, Task, Worker } from '../models'
import { mapMap, Dict, IResource } from '../utils'

export interface ApiOpts {
    id: string,
    logger: typeof console
    etcd: Namespace
    mesh: KyokoMesh
}

const WORKER_RPC = workerAPI({ } as any),
    EXEC_RPC = executorAPI({ } as any)

export const api = ({ etcd, mesh, logger }: ApiOpts) => ({
    async select(tags: string[], usage: Dict<IResource>) {
        const tagged = await Promise.all(tags.map(tag => etcd.namespace(`worker/tagged/${tag}/`).getAll().keys())),
            keys = Array.from(new Set(tagged.reduce((all, keys) => all.concat(keys), [ ]))),
            locks = await Promise.all(keys.map(id => etcd.get(`worker/dispatching/${id}`).string())),
            unlocked = keys.filter((_, index) => !locks[index]),
            registry = await Promise.all(unlocked.map(id => etcd.get(`worker/${id}`).json())),
            avail = registry.map(data => new Worker(data)).filter(worker => worker.canRun({ usage }))
        logger.log(`got ${keys.length} workers for tags "${tags}", ${avail.length} available`)
        return avail
    },
    async dispatch(job: Partial<Job>, step: string, worker: Partial<Worker>, tasks: Dict<Partial<Task>>) {
        logger.log(`job "${job.id}", step "${step}" is being dispatched to worker "${worker.id}"`)
        const lock = etcd.lock(`worker/dispatching/${worker.id}`)
        try {
            await lock.acquire()
            const started = await mesh.query(WORKER_RPC).workers[worker.id || ''].start(tasks)
            await Promise.all(started.map(id => etcd.put(`step/started/${job.id}/${step}/${id}`).value(JSON.stringify(tasks[id]))))
            await lock.release()
        } catch (err) {
            logger.error(`dispatch task failed`, err)
        }
    },
    async start(job: Partial<Job>, step: string) {
        const registry = await etcd.namespace(`step/started/${job.id}/${step}/`).getAll().json(),
            stp = new Step((job.steps || { })[step]),
            usage = await stp.usage({ step }, job),
            workers = await this.select(stp.tags, usage),
            started = mapMap(registry, data => new Task(data)),
            plans = await stp.plan(job, step, workers, started)
        logger.log(`job "${job.id}" got ${workers.length} workers, ${plans ? plans.length : 'no'} plans`)
        if (plans) {
            await Promise.all(plans.map(({ worker, tasks }) => this.dispatch(job, step, worker, tasks)))
        } else {
            await etcd.put(`step/done/${job.id}/${step}`).value(JSON.stringify(Object.keys(registry)))
            logger.log(`job "${job.id}", step "${step}" finished`)
        }
    },
    async update(id: string) {
        const job = new Job(await etcd.get(`job/submited/${id}`).json()),
            done = await etcd.namespace(`step/done/${id}/`).getAll().json(),
            steps = Object.keys(job.steps).filter(step => !done[step] && job.deps(step).every(dep => !!done[dep]))
        logger.log(`job "${id}", steps to run: ${steps.length ? steps : 'none'}`)
        if (steps.length) {
            await Promise.all(steps.map(step => this.start(job, step)))
        } else {
            await etcd.delete().key(`job/submited/${id}`)
            logger.log(`job "${id}" done`)
        }
    },
    async check() {
        const jobs = await etcd.namespace(`job/submited/`).getAll().keys()
        logger.log(`got ${jobs.length} jobs to update`)
        for (const id of jobs) {
            const lock = etcd.lock(`job/checking/${id}`)
            try {
                await lock.acquire()
                await this.update(id)
                await lock.release()
            } catch (err) {
                logger.error(`update task failed`, err)
            }
        }
    },
    async submit(job: Partial<Job>) {
        const id = job.id = job.id || Math.random().toString(16).slice(2, 10)
        await etcd.put(`job/submited/${id}`).value(JSON.stringify(job))
        logger.log(`submited job "${id}"`)
        return id
    },
    async kill(id: string, message: string) {
        const keys = await etcd.namespace(`task/executing/${id}/`).getAll().keys()
        await Promise.all(keys.map(key => mesh.query(EXEC_RPC).executors[key].kill()))
        logger.log(`killing job "${id}", tasks "${keys}", message: ${message}`)
    },
})

export default (opts: ApiOpts) => ({
    scheduler: api(opts),
})
