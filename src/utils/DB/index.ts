import { Sequelize } from 'sequelize';
import dotenvConfig from '../dotenv';

export let sequelize: Sequelize = null as unknown as Sequelize;

export async function init() {
  await dotenvConfig();
  if (!sequelize) {
    sequelize = new Sequelize(
      process.env.DB || 'grafana',
      process.env.DB_USER!,
      process.env.DB_PASS,
      {
        host: process.env.DB_HOST || 'localhost',
        port: Number(process.env.DB_PORT) || 5432,
        dialect: 'postgres',
        logging: false,
      },
    );
    if (!(await testConnection())) throw 'Connection Failed.';
    return true;
  }
  return false;
}

export async function testConnection() {
  try {
    await sequelize.authenticate();
    return true;
  } catch (error) {
    console.error(error);
    return false;
  }
}

export default {
  init,
  sequelize,
  testConnection,
};
