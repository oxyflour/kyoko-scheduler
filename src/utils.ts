export function memo<F extends (...args: any[]) => any>(func: F) {
    return ((...args: any[]) => {
        const keys = args.join('//'),
            self = memo as { cache?: any },
            cache = self.cache || (self.cache = { }),
            { ret } = cache[keys] || (cache[keys] = { ret: func(...args) })
        return ret
    }) as F
}

export function debounce<F extends (...args: any[]) => any>(func: F, delay: number) {
    let timeout = null as null | NodeJS.Timer
    return ((...args: any[]) => {
        if (timeout) {
            clearTimeout(timeout)
        }
        timeout = setTimeout(() => {
            if (timeout) {
                clearTimeout(timeout)
                timeout = null
            }
            func(...args)
        }, delay)
    }) as F
}

const formatter = memo((...args: string[]) => new Function(...args))
export function format(template: string, args: any) {
    const keys = Object.keys(args),
        vals = Object.values(args)
    return template.replace(/{{[^}]+}}/g, (_, match) => formatter(match, ...keys)(...vals))
}

export function arrMap<T, M>(array: T[], fn: (val: T, key: number) => M): { [K: string]: M } {
    return array.reduce((ret, val, index) => ({ ...ret, [val + '']: fn(val, index) }), { })
}

export function mapMap<T, M>(dict: { [K: string]: T }, fn: (val: T, key: string) => M): { [K: string]: M } {
    return Object.keys(dict).reduce((ret, key) => ({ ...ret, [key]: fn(dict[key], key) }), { })
}

export interface Dict<T> {
    [key: string]: T
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
    toPlain() {
        return this as { } as IResource
    }
    map(val: IResource, fn: (a: number, b: number) => number) {
        const a = this.toPlain(),
            b = { ...val, ...a }
        return new Resource(mapMap(b, (_, key) => Resource.mapVal(a[key], b[key], fn)))
    }
    add(val: IResource) {
        return this.map(val, (a, b) => a + b)
    }
    sub(val: IResource) {
        return this.map(val, (a, b) => a - b)
    }
    mul(val: number) {
        return this.map(mapMap(this.toPlain(), () => val), (a, b) => a * b)
    }
    avail() {
        return Object.values(this)
            .every(val => Array.isArray(val) ? val.every(val => val >= 0) : val >= 0)
    }
}

export class Usage {
    constructor(data = { } as Dict<IResource | Resource>) {
        Object.assign(this, data)
    }
    toPlain() {
        return this as { } as Dict<IResource>
    }
    at(time: number) {
        const usage = this.toPlain()
        if (usage[time]) {
            return usage[time]
        }
        const ticks = Object.keys(usage).map(time => parseInt(time)).sort((a, b) => a - b),
            res = Object.values(usage),
            index = ticks.findIndex((_, index) => ticks[index] <= time && time < ticks[index + 1])
        if (index >= 0) {
            const [t0, t1] = [ticks[index], ticks[index + 1]],
                [r0, r1] = [res[index], res[index + 1]],
                k = (time - t0) / (t1 - t0)
            return new Resource(r1).mul(k / (1 - k)).add(r0).mul(1 - k).toPlain()
        } else {
            return { }
        }
    }
    offset(time: number) {
        const total = { } as { [tick: string]: IResource },
            usage = this.toPlain()
        for (const tick in usage) {
            total[parseInt(tick) + time] = usage[tick]
        }
        return new Usage(total)
    }
    add(...args: Dict<IResource>[]) {
        const total = { } as { [tick: string]: Resource }
        for (const arg of args) {
            for (const tick in arg) {
                total[tick] = total[tick] || new Resource()
            }
        }
        for (const arg of args) {
            const usage = new Usage(arg)
            for (const tick in total) {
                total[tick] = total[tick].add(usage.at(parseInt(tick)))
            }
        }
        return new Usage(total)
    }
    below(res: IResource) {
        const total = new Resource(res)
        return Object.values(this).every(res => total.sub(res).avail())
    }
}
