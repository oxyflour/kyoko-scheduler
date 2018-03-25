module.exports = {
    steps: {
        A: {
            instances: 5,
            concurrency: 2,
            res: { cpu: 1 },
            cmd: 'ping localhost -n 5',
        },
        B: {
            deps: ['A'],
            instances: 3,
            concurrency: 3,
            res: { cpu: 1 },
            cmd: 'ping localhost -n 20',
        }
    }
}
