export function memo<F extends (...args: any[]) => any>(func: F) {
    return ((...args: any[]) => {
        const keys = args.join('//'),
            self = memo as { cache?: any },
            cache = self.cache || (self.cache = { }),
            { ret } = cache[keys] || (cache[keys] = { ret: func(...args) })
        return ret
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
