import {existsSync, readFileSync} from 'node:fs';
import {join} from 'node:path';

export function loadLuaScript(name: string): string {
  const local = join(__dirname, 'lua', name);
  // tsc preserves the project root under dist; source assets remain in src.
  const source = join(__dirname, '../../../src/store/lua', name);
  return readFileSync(existsSync(local) ? local : source, 'utf8');
}
