import KyokoMesh from 'kyoko-mesh'
import { Namespace } from 'etcd3'

import workerAPI from './worker'
import executorAPI from './executor'
import { Job, Step, Task, Worker, Dict, IResource, Resource } from '../models'
import { arrMap, mapMap } from '../utils'

export interface ApiOpts {
    id: string,
    logger: typeof console
    etcd: Namespace
    mesh: KyokoMesh
}

const WORKER_RPC = workerAPI({ } as any),
    EXEC_RPC = executorAPI({ } as any)

export const api = ({ etcd, mesh, logger }: ApiOpts) => ({
    async verify(worker: Partial<Worker>, usage: Dict<IResource>) {
        const working = await etcd.namespace(`working/${worker.id}/`).getAll().json(),
            tasks = Object.values(working).concat({ usage }).map(data => new Task(data)),
            output = { } as { [time: string]: Resource }
        for (const task of tasks) {
            for (const time in usage) {
                const tick = (task.created || 0) + parseInt(time)
                output[tick] = output[tick] || new Resource(worker.resource)
            }
        }
        for (const tick of Object.keys(output).map(tick => parseInt(tick))) {
            for (const res of tasks.map(task => task.res(tick)).filter(r => r) as IResource[]) {
                output[tick] = output[tick].sub(res)
            }
        }
        return Object.values(output).every(res => res.valid())
    },
    async select(tags: string[], res: Dict<IResource>) {
        const [head, ...rest] = await Promise
                .all(tags.map(tag => etcd.namespace(`tagged/${tag}/`).getAll().json())),
            workers = Object.keys(head || { })
                .filter(id => rest.every(dict => !!dict[id]))
                .map(id => new Worker(head[id])),
            verified = await Promise.all(workers.map(worker => this.verify(worker, res))),
            avail = workers.filter((_, index) => verified[index])
        logger.log(`got ${workers.length} workers for tags "${tags}", ${avail.length} available`)
        return avail
    },
    async dispatch(job: Partial<Job>, step: string, worker: Partial<Worker>, tasks: Dict<Partial<Task>>) {
        logger.log(`job "${job.id}", step "${step}" is being dispatched to worker "${worker.id}"`)
        const lock = etcd.lock(`dispatch-worker/${worker.id}`)
        try {
            await lock.acquire()
            await mesh.query(WORKER_RPC).workers[worker.id || ''].start(tasks)
            await lock.release()
        } catch (err) {
            logger.error(`dispatch task failed`, err)
            await lock.release()
        }
    },
    async start(j: Partial<Job>, step: string, deps: Dict<Dict<Partial<Task>>>) {
        const job = new Job(j),
            registry = await etcd.namespace(`job/${job.id}/started/${step}/`).getAll().json(),
            started = mapMap(registry, data => new Task(data)),
            stp = new Step(job.steps[step]),
            usage = await stp.usage({ job, step }),
            workers = await this.select(stp.tags, usage),
            plans = await stp.plan(job, step, deps, started, workers)
        logger.log(`job "${job.id}" got ${workers.length} workers, ${plans ? plans.length : 'no'} plans`)
        if (plans) {
            await Promise.all(plans.map(({ worker, tasks }) => this.dispatch(job, step, worker, tasks)))
        } else {
            await etcd.put(`success/${job.id}/${step}`).value(JSON.stringify(registry))
            logger.log(`job "${job.id}", step "${step}" finished`)
        }
    },
    async update(id: string) {
        const job = new Job(await etcd.get(`submited/${id}`).json()),
            registry = await etcd.namespace(`success/${job.id}/`).getAll().json(),
            success = mapMap(registry, map => mapMap(map as Dict<Task>, data => new Task(data))),
            steps = Object.keys(job.steps).filter(step => !success[step])
        logger.log(`job "${id}", steps to run: ${steps.length ? steps : 'none'}`)
        if (steps.length) {
            const deps = steps.map(step => job.deps(step)).map(deps => arrMap(deps, dep => success[dep]))
            await Promise.all(steps.map((step, index) => this.start(job, step, deps[index])))
        } else {
            await etcd.delete().key(`submited/${id}`).exec()
            logger.log(`job "${id}" done`)
        }
    },
    async check() {
        const jobs = await etcd.namespace(`submited/`).getAll().keys()
        logger.log(`got ${jobs.length} jobs to update`)
        for (const id of jobs) {
            const lock = etcd.lock(`check-task/${id}`)
            try {
                await lock.acquire()
                await this.update(id)
                await lock.release()
            } catch (err) {
                logger.error(`update task failed`, err)
                await lock.release()
            }
        }
    },
    async submit(job: Partial<Job>) {
        const id = job.id = job.id || Math.random().toString(16).slice(2, 10)
        await etcd.put(`submited/${id}`).value(JSON.stringify(job))
        logger.log(`submited job "${id}"`)
        return id
    },
    async kill(id: string) {
        await mesh.query(EXEC_RPC).executors[id].kill()
    },
})

export default (opts: ApiOpts) => ({
    scheduler: api(opts),
})
