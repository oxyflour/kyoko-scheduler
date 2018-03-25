#!/usr/bin/env

const program = require('commander'),
    fs = require('fs'),
    os = require('os'),
    path = require('path'),
    packageJson = require('./package.json'),
    Service = require('./dist/service').default,
    schedulerAPI = require('./dist/api/scheduler').default
    api = schedulerAPI({ })

program.version(packageJson.version)

program.command(`execute <task-id> <task-json>`)
    .action((taskId, taskData) => new Service({
        id: taskId,
        startExecutor: JSON.parse(taskData),
    }))

program.command(`serve-scheduler`)
    .action(() => new Service({
        startScheduler: true
    }))

program.command(`serve-worker`)
    .action(() => new Service({
        startWorker: true,
    }))

program.command(`serve`)
    .action(() => new Service({
        startScheduler: true,
        startWatcher: true,
        startWorker: true,
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

program.command(`update [job-id]`)
    .action(id => new Service().once('ready', async mesh => {
        try {
            if (id) {
                await mesh.query(api).scheduler.update(id)
            } else {
                await mesh.query(api).scheduler.check()
            }
            process.exit(0)
        } catch (err) {
            console.error(`Update job "${id}" failed:`, err)
            process.exit(-1)
        }
    }))

program.command(`kill <job-id>`)
    .action(id => new Service().once('ready', async mesh => {
        try {
            await mesh.query(api).scheduler.kill(id)
            process.exit(0)
        } catch (err) {
            console.error(`Update job "${id}" failed:`, err)
            process.exit(-1)
        }
    }))

program.parse(process.argv)
if (!process.argv.slice(2).length) {
    program.outputHelp()
    process.exit(-1)
}
