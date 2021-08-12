import {Attribute, Attributes, Statement, StringMap} from './metadata';

export function param(i: number): string {
  return '?';
}
export function params(length: number, from?: number): string[] {
  if (from === undefined || from == null) {
    from = 0;
  }
  const ps: string[] = [];
  for (let i = 1; i <= length; i++) {
    ps.push(param(i + from));
  }
  return ps;
}

export interface Metadata {
  keys: Attribute[];
  bools?: Attribute[];
  map?: StringMap;
  version?: string;
  fields?: string[];
}
export function metadata(attrs: Attributes): Metadata {
  const mp: StringMap = {};
  const ks = Object.keys(attrs);
  const ats: Attribute[] = [];
  const bools: Attribute[] = [];
  const fields: string[] = [];
  let ver: string;
  let isMap = false;
  for (const k of ks) {
    const attr = attrs[k];
    attr.name = k;
    if (attr.key) {
      ats.push(attr);
    }
    if (!attr.ignored) {
      fields.push(k);
    }
    if (attr.type === 'boolean') {
      bools.push(attr);
    }
    if (attr.version) {
      ver = k;
    }
    const field = (attr.field ? attr.field : k);
    const s = field.toLowerCase();
    if (s !== k) {
      mp[s] = k;
      isMap = true;
    }
  }
  const m: Metadata = {keys: ats, fields, version: ver};
  if (isMap) {
    m.map = mp;
  }
  if (bools.length > 0) {
    m.bools = bools;
  }
  return m;
}
export function buildToSave<T>(obj: T, table: string, attrs: Attributes, buildParam?: (i: number) => string, i?: number): Statement {
  if (!i) {
    i = 1;
  }
  if (!buildParam) {
    buildParam = param;
  }
  const ks = Object.keys(attrs);
  const cols: string[] = [];
  const values: string[] = [];
  const args: any[] = [];
  for (const k of ks) {
    let v = obj[k];
    const attr = attrs[k];
    if (attr && !attr.ignored) {
      if (attr.default !== undefined && attr.default != null && (v === undefined || v == null)) {
        v = attr.default;
      }
      if (v !== undefined) {
        const field = (attr.field ? attr.field : k);
        cols.push(field);
        if (v === '') {
          values.push(`''`);
        } else if (v == null) {
          values.push(`null`);
        } else if (typeof v === 'number') {
          values.push(toString(v));
        } else if (typeof v === 'boolean') {
          if (attr.true === undefined) {
            if (v === true) {
              values.push(`true`);
            } else {
              values.push(`false`);
            }
          } else {
            const p = buildParam(i++);
            values.push(p);
            if (v === true) {
              const v2 = (attr.true ? attr.true : '1');
              args.push(v2);
            } else {
              const v2 = (attr.false ? attr.false : '0');
              args.push(v2);
            }
          }
        } else {
          const p = buildParam(i++);
          values.push(p);
          args.push(v);
        }
      }
    }
  }
  if (cols.length === 0) {
    return null;
  } else {
    const query = `insert into ${table}(${cols.join(',')})values(${values.join(',')})`;
    return { query, params: args };
  }
}
export function buildToSaveBatch<T>(objs: T[], table: string, attrs: Attributes, buildParam?: (i: number) => string): Statement[] {
  if (!buildParam) {
    buildParam = param;
  }
  const sts: Statement[] = [];
  const meta = metadata(attrs);
  const pks = meta.keys;
  if (!pks || pks.length === 0) {
    return null;
  }
  const ks = Object.keys(attrs);
  for (const obj of objs) {
    let i = 1;
    const cols: string[] = [];
    const values: string[] = [];
    const args: any[] = [];
    for (const k of ks) {
      const attr = attrs[k];
      if (attr && !attr.ignored) {
        let v = obj[k];
        if (attr.default !== undefined && attr.default != null && (v === undefined || v == null)) {
          v = attr.default;
        }
        if (v !== undefined) {
          const field = (attr.field ? attr.field : k);
          cols.push(field);
          if (v === '') {
            values.push(`''`);
          } else if (v == null) {
            values.push(`null`);
          } else if (typeof v === 'number') {
            values.push(toString(v));
          } else if (typeof v === 'boolean') {
            if (attr.true === undefined) {
              if (v === true) {
                values.push(`true`);
              } else {
                values.push(`false`);
              }
            } else {
              const p = buildParam(i++);
              values.push(p);
              if (v === true) {
                const v2 = (attr.true ? attr.true : '1');
                args.push(v2);
              } else {
                const v2 = (attr.false ? attr.false : '0');
                args.push(v2);
              }
            }
          } else {
            const p = buildParam(i++);
            values.push(p);
            args.push(v);
          }
        }
      }
    }
    const q = `insert into ${table}(${cols.join(',')})values(${values.join(',')})`;
    const smt: Statement = { query: q, params: args };
    sts.push(smt);
  }
  return sts;
}
const n = 'NaN';
export function toString(v: number): string {
  let x = '' + v;
  if (x === n) {
    x = 'null';
  }
  return x;
}