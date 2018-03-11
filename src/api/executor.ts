import treeKill from 'tree-kill'
import { ChildProcess } from "child_process"
import { Namespace } from "etcd3"
import { promisify } from 'util'

export interface ApiOpts {
    id: string
    proc: ChildProcess
    etcd: Namespace
    logger: typeof console
}

const api = ({ etcd, id, proc }: ApiOpts) => ({
    async kill() {
        const lock = etcd.lock(`proc-stop/${id}`)
        await lock.acquire()
        try {
            await promisify(treeKill)(proc.pid, undefined, undefined)
            await lock.release()
        } catch (err) {
            await lock.release()
            throw err
        }
    },
})

export default (opts: ApiOpts) => ({
    executors: {
        [opts.id]: api(opts)
    }
})
