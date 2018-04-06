import treeKill from 'tree-kill'
import { ChildProcess } from 'child_process'
import { Namespace } from 'etcd3'
import { promisify } from 'util'

export interface ApiOpts {
    id: string
    proc: ChildProcess
    etcd: Namespace
    logger: typeof console
}

const api = ({ etcd, id, proc }: ApiOpts) => ({
    async kill() {
        const lock = etcd.lock(`task/killing/${id}`)
        try {
            await lock.acquire()
            await promisify(treeKill)(proc.pid, undefined, undefined)
            await lock.release()
        } catch (err) {
            await lock.release()
            throw err
        }
    },
})

export default (opts: ApiOpts) => ({
    __filename,
    executors: {
        [opts.id]: api(opts)
    }
})
