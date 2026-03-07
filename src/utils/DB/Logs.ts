import { sequelize, init as initDB } from '.';
import { DataTypes, Model } from 'sequelize';

export default class Logs extends Model {
  declare id: Date;
  declare data: string;
}

(async function () {
  await initDB();
  Logs.init(
    {
      id: { type: DataTypes.DATE, primaryKey: true },
      data: DataTypes.STRING(10485760),
    },
    {
      sequelize,
    },
  );
  try {
    await Logs.sync();
    console.log('Successfully synced Logs table!');
  } catch (err) {
    console.error('An error occurred while syncing the Logs table!', err);
  }
})();
