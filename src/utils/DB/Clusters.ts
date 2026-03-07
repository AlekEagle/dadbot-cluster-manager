import { sequelize, init as initDB } from '.';
import { DataTypes, Model } from 'sequelize';

export default class Clusters extends Model {
  declare id: Date;
  declare data: { [key: string]: any[] };
}

(async function () {
  await initDB();
  Clusters.init(
    {
      id: { type: DataTypes.DATE, primaryKey: true },
      data: DataTypes.JSON,
    },
    {
      sequelize,
    },
  );
  try {
    await Clusters.sync();
    console.log('Successfully synced Clusters table!');
  } catch (err) {
    console.error('An error occurred while syncing the Clusters table!', err);
  }
})();
