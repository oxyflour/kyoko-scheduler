#!/usr/bin/env

require('ts-node/register')

const program = require('commander'),
    fs = require('fs'),
    path = require('path'),
    packageJson = require('./package.json'),
    Service = require('./src/service').default,
    schedulerAPI = require('./src/api/scheduler').default
    opts = { },
    api = schedulerAPI({ })

program.version(packageJson.version)

program.command(`execute <task-id> <task-json>`)
    .action((taskId, taskData) => new Service({
        id: taskId,
        executor: {
            start: true,
            task: JSON.parse(taskData),
        }
    }))

program.command(`serve-scheduler`)
    .action(() => new Service({
        scheduler: { start: true }
    }))

program.command(`serve-worker`)
    .action(() => new Service({
        worker: { start: true }
    }))

program.command(`submit <job-file>`)
    .action(file => new Service().once('ready', async mesh => {
        try {
            const job = require(path.join(__dirname, file))
            console.log(await mesh.query(api).scheduler.submit(job))
            process.exit(0)
        } catch (err) {
            console.error(`Submit job failed:`, err)
            process.exit(-1)
        }
    }))

program.command(`update <job-id>`)
    .action(id => new Service().once('ready', async mesh => {
        try {
            await mesh.query(api).scheduler.update(id)
            process.exit(0)
        } catch (err) {
            console.error(`Update job "${id}" failed:`, err)
            process.exit(-1)
        }
    }))

program.command(`check`)
    .action(() => new Service().once('ready', async mesh => {
        try {
            await mesh.query(api).scheduler.check()
            process.exit(0)
        } catch (err) {
            console.error(`Check job "${id}" failed:`, err)
            process.exit(-1)
        }
    }))

program.parse(process.argv)
if (!process.argv.slice(2).length) {
    program.outputHelp()
    process.exit(-1)
}
