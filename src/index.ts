import {Client, QueryOptions, types} from 'cassandra-driver';
import {buildToSave, buildToSaveBatch} from './build';
import {Attribute, Attributes, Manager, Statement, StringMap} from './metadata';

export * from './metadata';
export * from './build';

export {buildToSave as buildToInsert};
export {buildToSaveBatch as buildToInsertBatch};

export class ClientManager implements Manager {
  constructor(public client: Client) {
    this.exec = this.exec.bind(this);
    this.execBatch = this.execBatch.bind(this);
    this.query = this.query.bind(this);
    this.queryOne = this.queryOne.bind(this);
    this.execScalar = this.execScalar.bind(this);
    this.count = this.count.bind(this);
  }
  exec(sql: string, args?: any[]): Promise<number> {
    return exec(this.client, sql, args);
  }
  execBatch(statements: Statement[], batchSize?: number, options?: QueryOptions): Promise<number> {
    return execBatch(this.client, statements, batchSize, options);
  }
  query<T>(sql: string, args?: any[], m?: StringMap, bools?: Attribute[], options?: QueryOptions): Promise<T[]> {
    return query(this.client, sql, args, m, bools, options);
  }
  queryOne<T>(sql: string, args?: any[], m?: StringMap, bools?: Attribute[], options?: QueryOptions): Promise<T> {
    return queryOne(this.client, sql, args, m, bools, options);
  }
  execScalar<T>(sql: string, args?: any[]): Promise<T> {
    return execScalar<T>(this.client, sql, args);
  }
  count(sql: string, args?: any[]): Promise<number> {
    return count(this.client, sql, args);
  }
}

export function execOneRawBatch<T>(client: Client, statements: Statement[], options?: QueryOptions): Promise<T[]> {
  return client.batch(statements, options ? options : { prepare: true }).then(result => {
    return result.rows as any;
  });
}
export function execOneBatch(client: Client, statements: Statement[], options?: QueryOptions): Promise<number> {
  return client.batch(statements, options ? options : { prepare: true }).then(r => toLength(r));
}
export async function execBatch<T>(client: Client, statements: Statement[], batchSize?: number, options?: QueryOptions): Promise<number> {
  if (batchSize && batchSize > 0 && statements.length > batchSize) {
    const arr = [];
    while (statements.length !== 0) {
      if (statements.length > batchSize) {
        arr.push(client.batch(statements.splice(0, batchSize), options ? options : { prepare: true }).then(r => toLength(r)));
      } else {
        arr.push(client.batch(statements.splice(0, statements.length),  options ? options : { prepare: true }).then(r => toLength(r)));
      }
    }
    return await handlePromises<T>(arr);
  } else {
    return client.batch(statements, options ? options : { prepare: true }).then(r => toLength(r));
  }
}
export function handlePromises<T> (arr: Promise<number>[]): Promise<number> {
  return Promise.all(arr).then(r => {
    let c = 0;
    r.forEach(item => {
      if (item) {
        c = c + item;
      }
    });
    return c;
  });
}
export async function execRawBatch<T>(client: Client, statements: Statement[], batchSize?: number, options?: QueryOptions): Promise<T[]> {
  if (statements.length > batchSize) {
    const arr = [];
    while (statements.length !== 0) {
      if (statements.length > batchSize) {
        arr.push(client.batch(statements.splice(0, batchSize), options ? options : { prepare: true })) ;
      } else {
        arr.push(client.batch(statements.splice(0, statements.length),  options ? options : { prepare: true })) ;
      }
    }
    return await handleArrayPromises<T>(arr);
  } else {
    return client.batch(statements, options ? options : { prepare: true }).then(result => {
      return result.rows as any;
    });
  }
}
export function toLength(r: types.ResultSet): number {
  if (!r || !r.rows) {
    return 0;
  } else {
    return r.rowLength;
  }
}
export function handleArrayPromises<T> (arrayPromise: Promise<types.ResultSet>[]): Promise<T[]> {
  return Promise.all(arrayPromise).then(result => {
    let arrRealResult = [];
    result.forEach(item => {
      if (item.rows) {
        arrRealResult = [... arrRealResult, ...item.rows];
      }
    });
    return arrRealResult;
  });
}

export function exec(client: Client, sql: string, args?: any[]): Promise<number> {
  const p = args && args.length > 0 ? toArray(args) : undefined;
  return client.execute(sql, p, {prepare: true}).then(r => 1);
}
export function execRaw<T>(client: Client, sql: string, args?: any[]): Promise<T> {
  const p = args && args.length > 0 ? toArray(args) : undefined;
  return client.execute(sql, p, {prepare: true}).then(r => {
    if (r.rows.length >= 1) {
      return r.rows[0] as any;
    } else {
      return null;
    }
  });
}
export function query<T>(client: Client, sql: string, args?: any[], m?: StringMap, bools?: Attribute[], options?: QueryOptions): Promise<T[]> {
  const p = args && args.length > 0 ? toArray(args) : undefined;
  return client.execute(sql, p, options).then(r => {
    return handleResults<T>(r.rows as any, m, bools);
  });
}
export function queryOne<T>(client: Client, sql: string, args?: any[], m?: StringMap, bools?: Attribute[], options?: QueryOptions): Promise<T> {
  const p = args && args.length > 0 ? toArray(args) : undefined;
  return client.execute(sql, p, options).then(r => {
    if (!r.rows || r.rows.length === 0) {
      return null;
    }
    const x = handleResults<T>(r.rows as any, m, bools);
    return x[0];
  });
}
export function execScalar<T>(client: Client, sql: string, args?: any[]): Promise<T> {
  return queryOne<T>(client, sql, args).then(r => {
    if (!r) {
      return null;
    } else {
      const keys = Object.keys(r);
      return r[keys[0]];
    }
  });
}
export function count(client: Client, sql: string, args?: any[]): Promise<number> {
  return execScalar<number>(client, sql, args);
}
export function save<T>(client: Client|((sql: string, args?: any[]) => Promise<number>), obj: T, table: string, attrs: Attributes, buildParam?: (i: number) => string, i?: number): Promise<number> {
  const stm = buildToSave(obj, table, attrs, buildParam, i);
  if (!stm) {
    return Promise.resolve(0);
  } else {
    if (typeof client === 'function') {
      return client(stm.query, stm.params);
    } else {
      return exec(client, stm.query, stm.params);
    }
  }
}
export function saveBatch<T>(client: Client|((statements: Statement[]) => Promise<number>), objs: T[], table: string, attrs: Attributes, batchSize?: number, options?: QueryOptions, buildParam?: (i: number) => string): Promise<number> {
  const stmts = buildToSaveBatch(objs, table, attrs, buildParam);
  if (!stmts || stmts.length === 0) {
    return Promise.resolve(0);
  } else {
    if (typeof client === 'function') {
      return client(stmts);
    } else {
      return execBatch(client, stmts, batchSize, options);
    }
  }
}

export const insert = save;
export const insertBatch = saveBatch;

export function toArray<T>(arr: T[]): T[] {
  if (!arr || arr.length === 0) {
    return [];
  }
  const p: T[] = [];
  const l = arr.length;
  for (let i = 0; i < l; i++) {
    if (arr[i] === undefined) {
      p.push(null);
    } else {
      p.push(arr[i]);
    }
  }
  return p;
}
export function handleResult<T>(r: T, m?: StringMap, bools?: Attribute[]): T {
  if (r == null || r === undefined || (!m && (!bools || bools.length === 0))) {
    return r;
  }
  handleResults([r], m, bools);
  return r;
}
export function handleResults<T>(r: T[], m?: StringMap, bools?: Attribute[]): T[] {
  if (m) {
    const res = mapArray(r, m);
    if (bools && bools.length > 0) {
      return handleBool(res, bools);
    } else {
      return res;
    }
  } else {
    if (bools && bools.length > 0) {
      return handleBool(r, bools);
    } else {
      return r;
    }
  }
}
export function handleBool<T>(objs: T[], bools: Attribute[]): T[] {
  if (!bools || bools.length === 0 || !objs) {
    return objs;
  }
  for (const obj of objs) {
    for (const field of bools) {
      const v = obj[field.name];
      if (v != null && v !== undefined && typeof v !== 'boolean') {
        const b = field.true;
        if (b == null || b === undefined) {
          // tslint:disable-next-line:triple-equals
          obj[field.name] = ('1' == v || 'T' == v || 'Y' == v);
        } else {
          // tslint:disable-next-line:triple-equals
          obj[field.name] = (v == b ? true : false);
        }
      }
    }
  }
  return objs;
}
export function map<T>(obj: T, m?: StringMap): any {
  if (!m) {
    return obj;
  }
  const mkeys = Object.keys(m);
  if (mkeys.length === 0) {
    return obj;
  }
  const obj2: any = {};
  const keys = Object.keys(obj);
  for (const key of keys) {
    let k0 = m[key];
    if (!k0) {
      k0 = key;
    }
    obj2[k0] = obj[key];
  }
  return obj2;
}
export function mapArray<T>(results: T[], m?: StringMap): T[] {
  if (!m) {
    return results;
  }
  const mkeys = Object.keys(m);
  if (mkeys.length === 0) {
    return results;
  }
  const objs = [];
  const length = results.length;
  for (let i = 0; i < length; i++) {
    const obj = results[i];
    const obj2: any = {};
    const keys = Object.keys(obj);
    for (const key of keys) {
      let k0 = m[key];
      if (!k0) {
        k0 = key;
      }
      obj2[k0] = (obj as any)[key];
    }
    objs.push(obj2);
  }
  return objs;
}
export function getFields(fields: string[], all?: string[]): string[] {
  if (!fields || fields.length === 0) {
    return undefined;
  }
  const ext: string[] = [];
  if (all) {
    for (const s of fields) {
      if (all.includes(s)) {
        ext.push(s);
      }
    }
    if (ext.length === 0) {
      return undefined;
    } else {
      return ext;
    }
  } else {
    return fields;
  }
}
export function buildFields(fields: string[], all?: string[]): string {
  const s = getFields(fields, all);
  if (!s || s.length === 0) {
    return '*';
  } else {
    return s.join(',');
  }
}
export function getMapField(name: string, mp?: StringMap): string {
  if (!mp) {
    return name;
  }
  const x = mp[name];
  if (!x) {
    return name;
  }
  if (typeof x === 'string') {
    return x;
  }
  return name;
}
export function isEmpty(s: string): boolean {
  return !(s && s.length > 0);
}
export function version(attrs: Attributes): Attribute {
  const ks = Object.keys(attrs);
  for (const k of ks) {
    const attr = attrs[k];
    if (attr.version) {
      attr.name = k;
      return attr;
    }
  }
  return undefined;
}
// tslint:disable-next-line:max-classes-per-file
export class CassandraWriter<T> {
  client?: Client;
  exec?: (sql: string, args?: any[]) => Promise<number>;
  map?: (v: T) => T;
  param?: (i: number) => string;
  constructor(client: Client | ((sql: string, args?: any[]) => Promise<number>), public table: string, public attributes: Attributes, toDB?: (v: T) => T, buildParam?: (i: number) => string) {
    this.write = this.write.bind(this);
    if (typeof client === 'function') {
      this.exec = client;
    } else {
      this.client = client;
    }
    this.param = buildParam;
    this.map = toDB;
  }
  write(obj: T): Promise<number> {
    if (!obj) {
      return Promise.resolve(0);
    }
    let obj2 = obj;
    if (this.map) {
      obj2 = this.map(obj);
    }
    const stmt = buildToSave(obj2, this.table, this.attributes, this.param);
    if (stmt) {
      if (this.exec) {
        return this.exec(stmt.query, stmt.params);
      } else {
        return exec(this.client, stmt.query, stmt.params);
      }
    } else {
      return Promise.resolve(0);
    }
  }
}
// tslint:disable-next-line:max-classes-per-file
export class CassandraBatchWriter<T> {
  pool?: Client;
  version?: string;
  execute?: (statements: Statement[]) => Promise<number>;
  map?: (v: T) => T;
  param?: (i: number) => string;
  size?: number;
  constructor(client: Client | ((statements: Statement[]) => Promise<number>), public table: string, public attributes: Attributes, batchSize?: number, toDB?: (v: T) => T, buildParam?: (i: number) => string) {
    this.write = this.write.bind(this);
    if (typeof client === 'function') {
      this.execute = client;
    } else {
      this.pool = client;
    }
    this.size = batchSize;
    this.param = buildParam;
    this.map = toDB;
    const x = version(attributes);
    if (x) {
      this.version = x.name;
    }
  }
  write(objs: T[]): Promise<number> {
    if (!objs || objs.length === 0) {
      return Promise.resolve(0);
    }
    let list = objs;
    if (this.map) {
      list = [];
      for (const obj of objs) {
        const obj2 = this.map(obj);
        list.push(obj2);
      }
    }
    const stmts = buildToSaveBatch(list, this.table, this.attributes, this.param);
    if (stmts && stmts.length > 0) {
      if (this.execute) {
        return this.execute(stmts);
      } else {
        return execBatch(this.pool, stmts, this.size);
      }
    } else {
      return Promise.resolve(0);
    }
  }
}

export interface AnyMap {
  [key: string]: any;
}
// tslint:disable-next-line:max-classes-per-file

export class CassandraChecker {
  constructor(private client: Client, private service?: string, private timeout?: number) {
    if (!this.timeout) {
      this.timeout = 4200;
    }
    if (!this.service) {
      this.service = 'cassandra';
    }
    this.check = this.check.bind(this);
    this.name = this.name.bind(this);
    this.build = this.build.bind(this);
  }
  async check(): Promise<AnyMap> {
    const obj = {} as AnyMap;
    const promise = new Promise<any>(async (resolve, reject) => {
      const state = this.client.getState();
      if (state.getConnectedHosts().length > 0) {
        resolve(obj);
      } else {
        reject(`Client down!`);
      }
    });
    if (this.timeout > 0) {
      return promiseTimeOut(this.timeout, promise);
    } else {
      return promise;
    }
  }
  name(): string {
    return this.service;
  }
  build(data: AnyMap, err: any): AnyMap {
    if (err) {
      if (!data) {
        data = {} as AnyMap;
      }
      data['error'] = err;
    }
    return data;
  }
}

function promiseTimeOut(timeoutInMilliseconds: number, promise: Promise<any>): Promise<any> {
  return Promise.race([
    promise,
    new Promise((resolve, reject) => {
      setTimeout(() => {
        reject(`Timed out in: ${timeoutInMilliseconds} milliseconds!`);
      }, timeoutInMilliseconds);
    })
  ]);
}
