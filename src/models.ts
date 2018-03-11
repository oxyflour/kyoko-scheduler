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
    deps = [ ] as string[]
    tags = [ ] as string[]
    count = 1
    cmd = ''
    cwd = ''
    env = { } as Dict<string>
    async require(_job: Partial<Job>, _step: string, _deps: Dict<Dict<Partial<Task>>>,
            _started: Dict<Partial<Task>>) {
        const t = 60 * 60 * 1000
        return {
            task: {
                created: Date.now()
            },
            res: {
                [0]: { cpu: 1 },
                [t]: { cpu: 1 },
            },
        } as Partial<Usage>
    }
    async plan(job: Partial<Job>, step: string, _deps: Dict<Dict<Partial<Task>>>,
            started: Dict<Partial<Task>>, workers: Partial<Worker>[]) {
        const rest = this.count - Object.keys(started).length
        if (rest > 0 && workers.length >= rest) {
            const plans = Array(rest).fill(Object.keys(started).length)
                .map((start, offset) => ({ job, index: start + offset }))
            return plans.map((arg, index) => {
                const worker = workers[index],
                    tasks = { } as Dict<Task>
                tasks[`j${job.id}-s${step}-w${worker.id}-t${arg.index}`] = new Task({
                    index: arg.index,
                    total: this.count,
                    cmd: format(this.cmd, arg),
                    cwd: format(this.cwd, arg),
                    env: Object.keys(this.env)
                        .reduce((env, key) => ({ ...env, [key]: format(this.env[key], arg) }), { }),
                    job, step, worker,
                })
                return { worker, tasks }
            })
        } else if (rest > 0) {
            return [ ]
        } else {
            return
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
}

export interface IResource {
    [type: string]: number[] | number
}

export class Resource {
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
    constructor(data = { } as IResource) {
        Object.assign(this, data)
    }
    map(val: IResource | number, fn: (a: number, b: number) => number) {
        const self = this as any as IResource,
            all = typeof val === 'number' ? self : { ...val, ...self }
        return new Resource(mapMap(all, (_, key) =>
            Resource.mapVal(self[key], typeof val === 'number' ? val : val[key], fn)))
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
        const self = this as any as IResource
        return Object.values(self)
            .every(val => Array.isArray(val) ? val.every(val => val >= 0) : val >= 0)
    }
}

export class Usage {
    constructor(data = { } as Partial<Usage>) {
        Object.assign(this, data)
    }
    task = { } as Partial<Task>
    res = { } as { [time: string]: IResource }
    getAt(time: number) {
        const { task, res } = this,
            ticks = Object.keys(res)
                .map(time => parseInt(time) + (task.created || 0))
                .sort((a, b) => a - b),
            index = ticks.findIndex((_, index) => ticks[index] < time && time <= ticks[index + 1]),
            [left, right] = [ticks[index], ticks[index + 1]]
        if (left < time && time <= right) {
            const k = (time - left) / (right - left)
            return new Resource(res[left]).mul((1 - k) / k).add(res[right]).mul(k)
        } else {
            return
        }
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
