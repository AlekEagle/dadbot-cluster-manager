import Logger, { Level } from './utils/Logger';
import { init as initDB } from './utils/DB';
import Clusters from './utils/DB/Clusters';
import Logs from './utils/DB/Logs';
import Errors from './utils/DB/Errors';
import Servers from './server';
import FS from 'node:fs';
import dotenvConfig from './utils/dotenv';
import Ajv from 'ajv';
import { DataTypes, GenericCloseCodes } from './types';
const ajv = new Ajv();
dotenvConfig();

const cdata: {
    stats: { [key: number]: any };
    count: number;
  } = {
    stats: {},
    count: -1,
  },
  schema = JSON.parse(FS.readFileSync('./config/schema.json', 'utf-8'));
let compiledSchema: any;

global.console = new Logger(
  process.env.DEBUG ? Level.DEBUG : Level.WARN,
) as any;

(async function () {
  try {
    // Compile the JSON schema for validating incoming data
    compiledSchema = ajv.compile(schema);
    // Initialize the database connection
    await initDB();
    // Set up event listeners for each server instance
    Servers.forEach((s) => {
      s.on('authenticated', (id, tC, u) => {
        cdata.count = tC;
        console.log(id, tC, u);
      });

      s.on('disconnected', (id, code) => {
        console.log(code, id);
      });
      s.on('data', (id, data, callback) => {
        switch (data.type) {
          case DataTypes.Stats:
            // Check if stats for this ID have already been received
            if (!cdata.stats[id]) {
              // Validate the incoming data against the compiled JSON schema
              if (!compiledSchema(data.data)) {
                console.error(`Invalid data received from ID ${id}:`);
                callback(false, GenericCloseCodes.InvalidData);
              } else {
                callback(true);
                cdata.stats[id] = data.data;
                console.debug(cdata);
                // Have we received stats from all expected clients?
                if (
                  new Array(cdata.count)
                    .fill(0)
                    .map((v, i, a) => i)
                    .every((v) => !!cdata.stats[v])
                ) {
                  // If we have stats from all clients, we can process and store the data
                  let statsEntriesArray = Array.from(
                    Object.entries(cdata.stats),
                  );
                  let b: { [key: string]: any } = {};
                  statsEntriesArray
                    .map((a) => a[1])
                    .forEach((aa) => {
                      Array.from(Object.entries(aa)).forEach((bb) => {
                        if (b[bb[0] as string] === undefined)
                          b[bb[0] as string] = [];
                        b[bb[0] as string].push(bb[1]);
                      });
                    });
                  Clusters.create({ id: Date.now(), data: b }).then(() => {
                    console.debug(cdata);
                    console.debug(b);
                    cdata.stats = {};
                    s.dataPushed();
                  });
                }
              }
              cdata.stats[id] = data.data;
            } else {
              callback(false, GenericCloseCodes.NotReadyForData);
            }
            break;
          case 1:
            Logs.create({ id: Date.now(), data: data.data }).then(
              () => {
                callback(true);
              },
              () => {
                callback(false, GenericCloseCodes.ServerError);
              },
            );
            break;
          case 2:
            Errors.create({ id: Date.now(), data: data.data }).then(
              () => {
                callback(true);
              },
              () => {
                callback(false, GenericCloseCodes.ServerError);
              },
            );
            break;
          default:
            callback(false, GenericCloseCodes.InvalidData);
        }
      });
    });
  } catch (e) {
    console.error(e);
    Servers.forEach((s) => {
      s.serverClosing();
    });
    process.exit(1);
  }
  process.on('beforeExit', (code) => {
    Servers.forEach((s) => {
      s.serverClosing();
    });
    process.exit(code);
  });
})();
