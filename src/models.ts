import { format, mapMap, IResource, Usage, Dict } from './utils'

export class Job {
    constructor(data = { } as Partial<Job>) {
        Object.assign(this, data)
    }
    id = ''
    cwd = ''
    created = Date.now()

    steps = { } as Dict<Partial<Step>>
    deps(step: string, out = { } as Dict<string>) {
        const deps = this.steps[step] && this.steps[step].deps || [ ]
        deps.filter(dep => !out[dep]).forEach(dep => this.deps(out[dep] = dep, out))
        return Object.keys(out)
    }
}

export class Step {
    constructor(data = { } as Partial<Step>) {
        Object.assign(this, data)
    }
    module = ''
    deps = [ ] as string[]
    tags = [ ] as string[]
    instances = 1
    concurrency = 1
    cmd = ''
    cwd = ''
    env = { } as Dict<string>
    res = { } as IResource
    async usage(task: Partial<Task>, job: Partial<Job>) {
        if (this.module) {
            const cls = require(this.module).default
            if (typeof cls !== 'function') {
                throw Error(`module "${this.module}" is not a class`)
            }
            return await Object.assign(new cls(this), this).usage(task, job) as Dict<IResource>
        } else {
            const t = Date.now() + 60 * 60 * 1000 - (task.created || Date.now())
            return { [0]: this.res, [t]: this.res } as Dict<IResource>
        }
    }
    async plan(job: Partial<Job>, step: string, workers: Partial<Worker>[], started: Dict<Partial<Task>>) {
        if (this.module) {
            const cls = require(this.module).default
            if (typeof cls !== 'function') {
                throw Error(`module "${this.module}" is not a class`)
            }
            const stp = Object.assign(new cls(this), this)
            return await stp.plan(job, step, started, workers) as {
                worker: Partial<Worker>,
                tasks: Dict<Task>,
            }[]
        } else {
            const all = Object.values(started),
                running = all.filter(task => !task.finished),
                successed = all.filter(task => task.finished && !task.error),
                rest = this.instances - (successed.length + running.length)
            // have enough workers to run?
            if (rest > 0 && workers.length >= this.concurrency) {
                return workers.slice(0, rest).map((worker, offset) => {
                    const tasks = { } as Dict<Partial<Task>>,
                        index = all.length + offset,
                        arg = { job, step, started, index },
                        id = `J${job.id}-S${step}-T${index}-${Math.random().toString(16).slice(2, 10)}`
                    tasks[id] = {
                        cmd: format(this.cmd, arg),
                        cwd: format(this.cwd, arg),
                        env: mapMap(this.env, val => format(val, arg)),
                        job: job.id,
                        step,
                        worker: worker.id,
                    }
                    return { worker, tasks }
                })
            // need more workers?
            } else if (rest > 0) {
                return [ ]
            // wait for all tasks to finish?
            } else if (running.length) {
                return [ ]
            // done
            } else {
                return
            }
        }
    }
}

export class Task {
    constructor(data = { } as Partial<Task>) {
        Object.assign(this, data)
    }

    created = Date.now()
    finished = 0
    error = ''
    code = 0

    cmd = ''
    cwd = ''
    env = { } as Dict<string>

    job = ''
    step = ''
    worker = ''

    // will be updated by executor automatically
    usage = { } as Dict<IResource>
}

export class Worker {
    constructor(data = { } as Partial<Worker>) {
        Object.assign(this, data)
    }
    id = ''

    total = { } as IResource
    canRun(...tasks: Partial<Task>[]) {
        const now = Date.now(),
            usage = tasks.map(({ usage, created }) => new Usage(usage).offset(created || now).toPlain())
        return new Usage(this.usage).add(...usage).below(this.total)
    }

    // will be update by worker automatically
    usage = { } as Dict<IResource>
}
