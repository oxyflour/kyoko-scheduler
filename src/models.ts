import { format, mapMap } from './utils'

export interface Dict<T> {
    [key: string]: T
}

export class Job {
    constructor(data = { } as Partial<Job>) {
        Object.assign(this, data)
    }
    id = ''
    cwd = ''
    created = Date.now()

    steps = { } as Dict<Partial<Step>>
    deps(step: string, out = { } as Dict<boolean>) {
        const deps = this.steps[step] && this.steps[step].deps || [ ]
        deps.filter(dep => !out[dep]).forEach(dep => this.deps(dep, out))
        return Object.keys(out)
    }
}

export class Step {
    constructor(data = { } as Partial<Step>) {
        Object.assign(this, data)
    }
    module = ''
    deps = [ ] as string[]
    tags = ['any']
    count = 1
    cmd = ''
    cwd = ''
    env = { } as Dict<string>
    res = null as IResource | null
    async usage(task: Partial<Task>) {
        if (this.module) {
            const cls = require(this.module).default
            if (typeof cls !== 'function') {
                throw Error(`module "${this.module}" is not a class`)
            }
            return await Object.assign(new cls(this), this).usage(task) as Dict<IResource>
        } else {
            const t = Date.now() + 60 * 60 * 1000 - (task.created || 0),
                res = this.res || { cpu: 1 } as IResource
            return { [0]: res, [t]: res } as Dict<IResource>
        }
    }
    async plan(job: Partial<Job>,
            step: string, deps: Dict<Dict<Partial<Task>>>,
            started: Dict<Partial<Task>>, workers: Partial<Worker>[]) {
        if (this.module) {
            const cls = require(this.module).default
            if (typeof cls !== 'function') {
                throw Error(`module "${this.module}" is not a class`)
            }
            const stp = Object.assign(new cls(this), this)
            return await stp.plan(job, step, deps, started, workers) as {
                worker: Partial<Worker>,
                tasks: Dict<Task>,
            }[]
        } else {
            const rest = this.count - Object.keys(started).length
            // have enough workers to run?
            if (rest > 0 && workers.length >= rest) {
                return workers.slice(0, rest).map((worker, offset) => {
                    const tasks = { } as Dict<Partial<Task>>,
                        index = Object.keys(started).length + offset,
                        arg = { job, step, deps, started, index }
                    tasks[`J${job.id}-S${step}-T${index}`] = {
                        index: index,
                        total: this.count,
                        cmd: format(this.cmd, arg),
                        cwd: format(this.cwd, arg),
                        env: mapMap(this.env, val => format(val, arg)),
                        job, step, worker,
                    }
                    return { worker, tasks }
                })
            // need more workers?
            } else if (rest > 0) {
                return [ ]
            // wait for all tasks to finish?
            } else if (Object.values(started).some(task => !task.finished)) {
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
    index = 0
    total = 0

    created = Date.now()
    finished = 0

    cmd = ''
    cwd = ''
    env = { } as Dict<string>

    job = { } as Partial<Job>
    step = ''
    worker = { } as Partial<Worker>

    usage = { } as { [time: string]: IResource }
    res(time: number) {
        const { usage, created } = this,
            ticks = Object.keys(usage)
                .map(time => parseInt(time) + (created || 0))
                .sort((a, b) => a - b),
            res = Object.values(usage),
            index = ticks.findIndex((_, index) => ticks[index] <= time && time < ticks[index + 1])
        if (index >= 0) {
            const [t0, t1] = [ticks[index], ticks[index + 1]],
                [r0, r1] = [res[index], res[index + 1]],
                k = (time - t0) / (t1 - t0)
            return new Resource(r1).mul(k / (1 - k)).add(r0).mul(1 - k)
        } else {
            return
        }
    }
}

export interface IResource {
    [type: string]: number[] | number
}

export class Resource implements IResource {
    static mapVal<T>(a: number[] | number, b: number[] | number, f: (a: number, b: number) => T) {
        if (Array.isArray(a)) {
            const c = Array.isArray(b) ? b : Array(a.length).fill(b)
            return a.map((_, i) => f(a[i] || 0, c[i] || 0))
        } else if (Array.isArray(b)) {
            return b.map((_, i) => f(b[i] || 0, a || 0))
        } else {
            return f(a || 0, b || 0)
        }
    }
    [type: string]: any
    constructor(data = { } as IResource) {
        Object.assign(this, data)
    }
    map(val: IResource | number, fn: (a: number, b: number) => number) {
        const self = typeof val === 'number' ? this : { ...val, ...(this as IResource) }
        return new Resource(mapMap(self, (_, key) =>
            Resource.mapVal((this as IResource)[key], typeof val === 'number' ? val : val[key], fn)))
    }
    add(val: IResource) {
        return this.map(val, (a, b) => a + b)
    }
    sub(val: IResource) {
        return this.map(val, (a, b) => a - b)
    }
    mul(val: number) {
        return this.map(val, (a, b) => a * b)
    }
    valid() {
        return Object.values(this)
            .every(val => Array.isArray(val) ? val.every(val => val >= 0) : val >= 0)
    }
}

export class Worker {
    constructor(data = { } as Partial<Worker>) {
        Object.assign(this, data)
    }
    id = ''
    updated = Date.now()
    resource = { } as IResource
}
