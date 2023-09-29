import { IDbPlugin, IDbPluginOptions, IBulkOperationsResponse, IDbRecord, IQueryParams, IDbRecordBase } from '@agrejus/db-framework';
import { v4 as uuidv4 } from 'uuid';

const memoryStore: { [key: string]: IDbRecordBase } = {}

export class MemoryDbPlugin<TDocumentType extends string, TEntityBase extends IDbRecord<TDocumentType>, TDbPluginOptions extends IDbPluginOptions = IDbPluginOptions> implements IDbPlugin<TDocumentType, TEntityBase> {

    protected readonly options: TDbPluginOptions;

    constructor(options: TDbPluginOptions) {
        this.options = options;
    }

    async destroy() {
        for(const key in memoryStore) {
            delete memoryStore[key]
        }
    }

    async all(payload?: IQueryParams<TDocumentType>) {

        const docs = Object.values<IDbRecordBase>(memoryStore);

        if (payload != null && payload.DocumentType) {
            return docs.filter(w => w.DocumentType === payload.DocumentType) as TEntityBase[]
        }

        return docs as TEntityBase[];
    }

    async getStrict(...ids: string[]) {

        return ids.reduce((a,v) => {

            if (memoryStore[v] == null) {
                throw new Error(`Document not found for id.  ID: ${v}`)
            }

            return [...a, memoryStore[v]] as TEntityBase[]

        },[] as TEntityBase[])
    }

    async get(...ids: string[]) {
        return ids.reduce((a,v) => {

            if (memoryStore[v] == null) {
                return a;
            }

            return [...a, memoryStore[v]] as TEntityBase[]

        },[] as TEntityBase[])
    }

    private _createRev(previousRev?: string) {
        const nextId = uuidv4();

        if (previousRev) {
            let count = Number(previousRev.substring(0, 1));
            return `${++count}-${nextId}`;
        }

        return `1-${nextId}`;
    }

    async bulkOperations(operations: { adds: TEntityBase[]; removes: TEntityBase[]; updates: TEntityBase[]; }) {
        const { adds, removes, updates } = operations;
        const result: IBulkOperationsResponse = {
            errors: {},
            errors_count: 0,
            successes: {},
            successes_count: 0
        }

        for(const add of adds) {
            try {
                (add as any)._rev = this._createRev();
                memoryStore[add._id] = add;

                result.successes_count++;
                result.successes[add._id] = {
                    id: add._id,
                    ok: true,
                    rev: add._rev
                }
            } catch (e: any) {
                result.errors_count++;
                result.errors[add._id] = {
                    id: add._id,
                    ok: false,
                    rev: add._rev,
                    error: e
                }
            }
        }

        for(const remove of removes) {
            try {
                delete memoryStore[remove._id];

                result.successes_count++;
                result.successes[remove._id] = {
                    id: remove._id,
                    ok: true,
                    rev: remove._rev
                }
            } catch (e:any) {
                result.errors_count++;
                result.errors[remove._id] = {
                    id: remove._id,
                    ok: false,
                    rev: remove._rev,
                    error: e
                }
            }
        }

        for(const update of updates) {
            try {
                (update as any)._rev = this._createRev();
                memoryStore[update._id] = update;

                result.successes_count++;
                result.successes[update._id] = {
                    id: update._id,
                    ok: true,
                    rev: update._rev
                }
            } catch (e:any) {
                result.errors_count++;
                result.errors[update._id] = {
                    id: update._id,
                    ok: false,
                    rev: update._rev,
                    error: e
                }
            }
        }

        return result;
    }
}