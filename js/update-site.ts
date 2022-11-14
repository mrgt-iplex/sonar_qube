import { BatteryRecord } from './../types/battery-record';
import { string } from 'prop-types';
import Transaction, { CheckoutCtx, OperationCtx } from './_base';
import { Store } from '..';
import createDataStore from '../../data-store';
import { Site } from '../types/site';
import { PowerPlant } from '../types/power-plant';
import { PlantReading } from '../types/shared';
import { Routine } from '../types/routine';
import { BatteryType } from '../types/battery-type';
import { Battery } from '../types/battery';
import { findByDate } from './utils';
import createLogger from '../../logger';
import { MONGO_URL, JWT_SECRET, INTERCOM_SECRET, APP_URL, API_COMPANY, SERVER_PORT, S3_BUCKET, S3_REGION, AUTH_SECRET, FILE_STORE_TYPE, FILE_ROOT, API_URL, MAIL_HOST, MAIL_USER, MAIL_PORT, MAIL_PASS, MAILER_TYPE } from '../../envs';
import { calcUtilization, findUtilizationStatus } from 'dugo-lib/lib/computers/power-plant';
import { calcCapacity as bCalcCapacity, findConductanceStatus, findTempStatus } from 'dugo-lib/lib/computers/battery';
import { calcCapacity as bPCalcCapacity, calcRuntime, findRuntimeStatus, findRuntimeThresholds, calcFloatVoltagePerJar, calcNominalFloatVoltageRanges, findFloatVoltageStatus, findYoungestBatteryTypeByString } from 'dugo-lib/lib/computers/battery-plant';
import { PlantConfig, BatteryString } from '../types/plant-config';
import { PlantRecord } from '../types/plant-record';
import { SiteConfig } from '../types/site-config';
import { Generator } from '../types/generator';
import { CommentUpdate } from '../types/log-item';
import { CompanyConfig } from '../types/company-config';
import { SiteUserAssociation } from '../types/site-user-association';
import { LogItem } from '../types/log-item';
import * as moment from 'moment';
import percisionRound from 'dugo-lib/lib/utils/percision-round';
import generator from '../schemas/generator';
import { getBatteryTypeIdByString } from '../functions/plant-battery-info';
import { getSns, getSnStatus} from '../../utils/serial-number';
import calcPercent from 'dugo-lib/lib/utils/calc-percent';
import { calcActualCapacity } from 'dugo-lib/lib/computers/string';
import { createConnection } from 'mongoose';
import { info } from 'winston';

const logger = createLogger({ label: 'Update Stats' });

interface SiteUpdates {
    siteNum: string;
    locationType: string;
    name: string;
    region: string;
    coords: any; // type update?
    address: string;
    generatorAction: string;
    generator: string;
    accessInstructions: string;
    notes: string;
}

interface PlantUpdates {
    plantNum: string;
    latestReading: any; // type update?
    transmission: string;
    technologyFlags: string[];
    serviceLevel: string;
}

interface RoutineUpdates {
    date: string | Date;
    latestReading?: any; // type update?
}

interface SerialNumberUpdates {
    batteryId: string;
    serialNumber: string;
}

interface BatteryUpdates {
    serialNumbers: [SerialNumberUpdates];
    batteriesIdsSupplimental: [string];
}

interface GeneralUpdates {
    primaryTech: string;
}

interface CommentInput {
    serialNumberUpdate?: number,
    commentUpdates?: any
}

type UpdateSiteTransactionInput = {
    siteUpdates: SiteUpdates;
    plantUpdates?: PlantUpdates;
    routineUpdates?: RoutineUpdates;
    batteryUpdates?: BatteryUpdates;
    generalUpdates?: GeneralUpdates;
    companyConfig?: CompanyConfig;
  };

  type CCtx = CheckoutCtx<Store, UpdateSiteTransactionInput>;
  type OCtx = OperationCtx<
    Store,
    UpdateSiteTransactionInput,
    UpdateSiteTransaction['checkout'] extends (ctx: any) => Promise<infer D> ?
      D : never
  >;

  export default class UpdateSiteTransaction extends Transaction<UpdateSiteTransactionInput, string> {
    protected async checkout ({ checkout, query, queryIds, getStrict, input }: CCtx) {
        const { siteUpdates, generalUpdates } = input;
        const { siteNum } = siteUpdates;
        const [ siteId ] = await queryIds('sites', { filter: { siteNum }, limit: 1 });
        let plantId: string | undefined;
        let powerPlant: PowerPlant | undefined;
        let selectedPlantConfig: PlantConfig | undefined;
        let siteConfig: SiteConfig | undefined;
        let generator: Generator | undefined;
        let plantConfig: PlantConfig | undefined;
        let plantRecord: PlantRecord | undefined;
        let routine: Routine | undefined;
        let batteries: Battery[] | undefined;

        if (!siteId) {
            throw new Error('Update Site Transaction: no site id found');
        }

        const site: Site = await checkout('sites', siteId!);

        const sitePlantIds = await queryIds('powerPlants', { filter: { site: siteId } });
        const sitePowerPlants = await Promise.all(
            sitePlantIds.map((id) => checkout('powerPlants', id!))
        );

        if (siteUpdates.generatorAction) {
            const now = new Date();
            const _siteConfig = await findByDate('siteConfigs', query, now, { site: siteId, isCurrent: true });
            siteConfig = await checkout('siteConfigs', _siteConfig.id);
            if (siteUpdates.generatorAction === 'remove' && siteConfig && siteConfig.generator) {
                generator = await checkout('generators', siteConfig.generator);
            }
        }

        const sitePlantBatteryInfoIds: string[] = [];
        const sitePlantConfigIds: string[] = [];
        for (const _plantId of sitePlantIds) {
            const _plantBatteryInfoIds = await queryIds('plantBatteryInfos', { filter: { powerPlant: _plantId } });
            if (_plantBatteryInfoIds.length) {
                _plantBatteryInfoIds.forEach((id) => {
                    sitePlantBatteryInfoIds.push(id!);
                });
            }

            const _plantConfigIds = await queryIds('plantConfigs', { filter: { powerPlant: _plantId } });
            if (_plantConfigIds.length) {
                _plantConfigIds.forEach((id) => {
                    sitePlantConfigIds.push(id!);
                });
            }
        }
        const sitePlantBatteryInfos = await Promise.all(
            sitePlantBatteryInfoIds.map((id) => checkout('plantBatteryInfos', id!))
        );
        const sitePlantConfigs = await Promise.all(
            sitePlantConfigIds.map((id) => checkout('plantConfigs', id!))
        );

        let primaryTechs: SiteUserAssociation[] = [];
        if (generalUpdates && generalUpdates.primaryTech) {
            const roleType = 'primary-technician';
            const [ roleTypeId ] = await queryIds('siteUserAssociationTypes', {
                filter: { name: roleType }
            });
            if (!roleTypeId) {
                throw new Error('No association type "Primary Technician" found.');
            }
            const primaryTechIds = await queryIds('siteUserAssociations', { filter: { site: site.id, associationType: roleTypeId } });
            primaryTechs = await Promise.all(
                primaryTechIds.map((id) => checkout('siteUserAssociations', id!))
            );
        }

        if (input.plantUpdates) {
            const plantIds = await queryIds('powerPlants', { filter: { site: siteId, name: input.plantUpdates!.plantNum }, limit: 1 });
            if (plantIds[0]) {
                plantId = plantIds[0];
                if (sitePlantIds.includes(plantId)) {
                    powerPlant = sitePowerPlants.filter((plant) => plant.id === plantId)[0];
                }
                else {
                    powerPlant = await checkout('powerPlants', plantId!);
                }
            }
            if (input.routineUpdates) {
                // convert here to preserve correct type
                input.routineUpdates.date = moment(input.routineUpdates.date).toDate();
                const routineDate: Date = input.routineUpdates.date;
                selectedPlantConfig = await findByDate('plantConfigs', query, routineDate, { powerPlant: plantId! });
                if (selectedPlantConfig) {
                    if (sitePlantConfigIds.includes(selectedPlantConfig.id)) {
                        plantConfig = sitePlantConfigs.filter((config) => config.id === selectedPlantConfig!.id)[0];
                    }
                    else {
                        plantConfig = await checkout('plantConfigs', selectedPlantConfig.id);
                    }
                }
            } else {
                const [ plantConfigId ] = await queryIds('plantConfigs', { filter: { powerPlant: plantId, isCurrent: true }, limit: 1});
                if (plantConfigId) {
                    if (sitePlantConfigIds.includes(plantConfigId)) {
                        plantConfig = sitePlantConfigs.filter((config) => config.id === plantConfigId)[0];
                    }
                    else {
                        plantConfig = await checkout('plantConfigs', plantConfigId);
                    }
                }
            }
            const [ plantRecordId ] = await queryIds('plantRecords', { filter: { powerPlant: plantId }, limit: 1});
            if (plantRecordId) {
                plantRecord = await checkout('plantRecords', plantRecordId);
            }

            if (input.routineUpdates) {
                const routineDate: Date = new Date(input.routineUpdates.date);
                const routineRead = await findByDate('routines', query, routineDate, { powerPlant: plantId! });
                routine = await checkout('routines', routineRead!.id);
            }
        }

        if (input.batteryUpdates && input.batteryUpdates.serialNumbers && input.batteryUpdates.serialNumbers.length) {
            batteries = await Promise.all(
                input.batteryUpdates.serialNumbers.map((datum) => checkout('batteries', datum.batteryId))
            );
        }

        return {
            site,
            siteConfig,
            generator,
            powerPlant,
            sitePowerPlants,
            sitePlantBatteryInfos,
            sitePlantConfigs,
            plantRecord,
            plantConfig,
            routine,
            batteries,
            primaryTechs
        };
    }

    protected async operation(ctx: OCtx): Promise<string> {
        const {
            powerPlant,
            routine,
            site,
            plantConfig,
            batteries
        } = ctx.data;
        let worstBlockConductanceHealthNew: number;
        const stringActualCapacitiesLol: number[] = [];
        let stringWorstAdmittanceHealth: number=0;
        let newActualCapacity = 0;
        let oldestBlockManufacturingDate: Date=new Date();
        const connection = await createConnection(MONGO_URL);
        
        const store = await createDataStore(
            connection,
            {
                jwtSecret: JWT_SECRET,
                authSecret: AUTH_SECRET,
                intercomSecret: INTERCOM_SECRET,
            }
            );
            
            const companyConfigg  = store.companyConfigs.readCurrent();
            
            for(let i=0;i<plantConfig!.strings.length;i++)
            {
            let battRecord = plantConfig!.strings[i];
            for(let j=0;j<battRecord.batteries.length;j++)
            {
                let batteryBlockId = battRecord.batteries[j];
                let battery = await ctx.readStrict('batteries',batteryBlockId);
                let batteryRecord = await ctx.readStrict('batteryRecords', battery.currentRecord);
                let batteryType = await ctx.readStrict('batteryTypes', battRecord.batteryType);
                console.log('batteryType: ', batteryType);
                let batteryConductance = batteryRecord.conductance[batteryRecord.conductance.length - 1].reading;
                const conductanceHealth = batteryType.conductance ? calcPercent(
                    batteryConductance, batteryType.conductance!
                    ) : 0;
                    console.log('conductanceHealth: ', conductanceHealth);
                    if (
                        worstBlockConductanceHealthNew! == undefined                    ) {
                        worstBlockConductanceHealthNew = conductanceHealth;
                    }
                    console.log('worstBlockConductanceHealthNew: ', worstBlockConductanceHealthNew);
                    if (
                        stringWorstAdmittanceHealth !== undefined
                                            ) {
                        stringWorstAdmittanceHealth = conductanceHealth;
                    }
                    console.log('stringWorstAdmittanceHealth: ', stringWorstAdmittanceHealth);
                    if (
                        oldestBlockManufacturingDate! === undefined ||
                        battery.manufacturingDate < oldestBlockManufacturingDate
                    ) {
                        oldestBlockManufacturingDate = battery.manufacturingDate;
                    }
                    const capacity = bCalcCapacity(
                        stringWorstAdmittanceHealth,
                        companyConfigg.batteryCapacityTable
                        );
                      if (capacity) {
                        stringActualCapacitiesLol[j] = batteryType.capacity ? calcActualCapacity(
                            capacity,
                            batteryType.capacity!
                            ) : 0;
                        }
                }
                
            
        }
         newActualCapacity = bPCalcCapacity(stringActualCapacitiesLol);

        const {
            siteUpdates,
            plantUpdates,
            routineUpdates,
            batteryUpdates,
            companyConfig,
            generalUpdates
        } = ctx.input;
        
        let commentInput: CommentInput = {};

        // check previous state condition
        let prevCondition: number | undefined;
        let reading: PlantReading | undefined;
        if (routine && routine.plantReading) {
            reading = routine.plantReading;
        }
        else if (routine && routine.latestReading) {
            reading = routine.latestReading;
        }

        if (reading && routineUpdates  && plantConfig) {
            const {
                load,
                voltage,
                temperature,
                utilization,
                actualCapacity,
                worstBlockConductanceHealth,
            } = reading;
            if(reading.actualCapacity == 0 && newActualCapacity != 0) 
            {
                reading.actualCapacity = newActualCapacity;
                reading.worstBlockConductanceHealth = worstBlockConductanceHealthNew;
            }
            console.log('reading: ', reading);

            prevCondition = await calculateRoutineCondition(
                ctx,
                site,
                routineUpdates.date,
                routine,
                plantConfig,
                companyConfig,
                load,
                voltage,
                temperature,
                utilization,
                actualCapacity,
                worstBlockConductanceHealth,
            ).catch((error) => {
                logger.error(error);
                return undefined;
            });
        }

        // perform site updates
        const updatedSite = await this.siteOperations(ctx);
        ctx.input = Object.assign(ctx.input, updatedSite);

        if (generalUpdates && generalUpdates.primaryTech) {
            await this.assignPrimaryTech(ctx);
        }

        if (powerPlant && plantUpdates) {

            // perform plant updates
            const updatedPlant = await this.plantOperations(ctx);
            ctx.input = Object.assign(ctx.input, updatedPlant);
            commentInput = updatedPlant.message;
            if (routine && routineUpdates) {

                // perform routine updates
                const updatedRoutine = await this.routineOperations(ctx);
                ctx.input = Object.assign(ctx.input, updatedRoutine);
            }
        }

        if (siteUpdates.generatorAction) {
            //perform gernerator updates
            const updatedSiteGenerator = await this.generatorOperations(ctx);
            Object.assign(updatedSite, updatedSiteGenerator);
            ctx.input = Object.assign(ctx.input, updatedSite);

            // perform site-config updates
            const updatedSiteConfig = await this.siteConfigOperations(ctx);
            ctx.input = Object.assign(ctx.input, updatedSiteConfig);
        }

        // perform battery updates
        if (batteries && batteries.length && batteryUpdates) {
            const updatedBattery = await this.batteryOperations(ctx);
            ctx.input = Object.assign(ctx.input, updatedBattery);

            if (updatedBattery.message) {
                commentInput.serialNumberUpdate = updatedBattery.message;
            }
        }

        // check new state condition
        let newReading: PlantReading | undefined;
        if (routine && routine.plantReading) {
            newReading = routine.plantReading;
        }
        else if (routine && routine.latestReading) {
            newReading = routine.latestReading;
        }
        if (newReading && routineUpdates && plantConfig) {
            if(newReading.actualCapacity == 0 && newActualCapacity != 0) 
            {
                newReading.actualCapacity = newActualCapacity;
                newReading.worstBlockConductanceHealth = worstBlockConductanceHealthNew;
            }
            const {
                load,
                voltage,
                temperature,
                utilization,
                actualCapacity,
                worstBlockConductanceHealth,
            } = newReading;


            const newCondition = await calculateRoutineCondition(
                ctx,
                site,
                routineUpdates.date,
                routine,
                plantConfig,
                companyConfig,
                load,
                voltage,
                temperature,
                utilization,
                actualCapacity,
                worstBlockConductanceHealth,
            ).catch((error) => {
                logger.error(error);
                return undefined;
            });

            if (
                (prevCondition || prevCondition === 0) &&
                (newCondition || newCondition === 0) &&
                (newCondition !== prevCondition)
            ) {
                const comment = {
                    readingType: 'condition',
                    prev: prevCondition.toString(),
                    new: newCondition.toString(),
                    manual: false
                };
                commentInput.commentUpdates.push(comment);
            }
        }

        // perform region update
        if (siteUpdates.region) {
            await this.regionOperations(ctx);
        }

        // perform comment update
        if (commentInput && (commentInput.serialNumberUpdate || (commentInput.commentUpdates && commentInput.commentUpdates.length))) {
            await this.commentOperations(ctx, commentInput);
        }
        return '';
    }

    async assignPrimaryTech (ctx: OCtx) {
        const { site } = ctx.data;
        const { generalUpdates } = ctx.input;
        if (generalUpdates) {
            const { primaryTech } = generalUpdates;
            const roleType = 'primary-technician';
            const [ roleTypeId ] = await ctx.queryIds('siteUserAssociationTypes', {
                filter: { name: roleType }
            });
            if (!roleTypeId) {
                throw new Error('No association type "Primary Technician" found.');
            }

            const primaryTechs = ctx.data.primaryTechs;
            if (primaryTechs.length) {
                 for (const _primaryTech of primaryTechs) {
                    ctx.remove(_primaryTech);
                }
            }

            await ctx.create('siteUserAssociations', {
                user: primaryTech,
                site: site.id,
                associationType: roleTypeId,
            });
        }
        return;
    }

    async regionOperations (ctx: OCtx) {
        const {
            site,
            powerPlant,
            sitePowerPlants,
            sitePlantBatteryInfos,
            sitePlantConfigs,
        } = ctx.data;

        const {
            siteUpdates
        } = ctx.input;

        const region  = siteUpdates.region;

        site.region = region;

        if (powerPlant) {
            powerPlant.region = region;
        }

        if (sitePowerPlants.length) {
            for (const plant of sitePowerPlants) {
                plant.region = region;
            }
        }

        if (sitePlantBatteryInfos.length) {
            for (const info of sitePlantBatteryInfos) {
                info.region = region;
            }
        }
        if (sitePlantConfigs.length) {
            for (const config of sitePlantConfigs) {
                config.region = region;
            }
        }

        return;
    }

    async siteOperations(ctx: OCtx) {
        const {
            site
        } = ctx.data;

        const {
            siteUpdates
        } = ctx.input;

        if (siteUpdates.locationType &&
            (siteUpdates.locationType === 'urban' || siteUpdates.locationType === 'rural')
        ) {
            site.locationType = siteUpdates.locationType;
        }

        if (siteUpdates.name) {
            site.name = siteUpdates.name;
        }

        if (siteUpdates.coords) {
            const location = {
                type: 'point',
                coordinates: siteUpdates.coords
            };
            site.location = location;
        }

        if (siteUpdates.address) {
            site.address = siteUpdates.address;
        }
        if (siteUpdates.accessInstructions) {
            site.accessInstructions = siteUpdates.accessInstructions;
        }

        if (siteUpdates.notes) {
            site.notes = siteUpdates.notes;
        }

        const updatedSite = Object.assign(site, siteUpdates);
        return {
            siteUpdates
        };
    }

    async generatorOperations(ctx: OCtx) {
        const {
            site,
            generator
        } = ctx.data;

        const {
            siteUpdates
        } = ctx.input;
        let _generator: Generator | undefined;

        if (generator && siteUpdates.generatorAction && siteUpdates.generatorAction === 'remove') {
            generator.removalDate = new Date();
            _generator = generator;
        }
        else if (siteUpdates.generatorAction && siteUpdates.generatorAction === 'add') {
            _generator = await ctx.create('generators', {
                site: site.id,
                installDate: new Date(),
            });
        }

        siteUpdates.generator = _generator!.id;
        const updatedSite = Object.assign(site, siteUpdates);
        return {
            siteUpdates
        };
    }

    async siteConfigOperations(ctx: OCtx) {
        const {
            siteConfig,
            site,
        } = ctx.data;

        const {
            siteUpdates,
        } = ctx.input;

        if (siteUpdates.generatorAction && siteConfig) {
            if (siteUpdates.generatorAction === 'remove') {
                siteConfig.isCurrent = false;
                await ctx.create('siteConfigs', {
                    ...siteConfig,
                    date: new Date(),
                    isCurrent: true,
                    generator: undefined,
                });
            }
            else if (siteUpdates.generatorAction === 'add' && siteUpdates.generator) {
                siteConfig.isCurrent = false;
                await ctx.create('siteConfigs', {
                   ...siteConfig,
                    date: new Date(),
                    isCurrent: true,
                    generator: siteUpdates.generator
                });
            }
        }

        const updatedSiteConfig = Object.assign(siteConfig, {...siteUpdates, generator: siteUpdates.generatorAction === 'remove' ? siteUpdates.generator : undefined});
        return {
            updatedSiteConfig
        };
    }

    async plantOperations(ctx: OCtx) {
        const {
            powerPlant,
            plantRecord,
            plantConfig,
            routine,
            site
        } = ctx.data;
        const {
            plantUpdates,
            routineUpdates,
            companyConfig,
        } = ctx.input;
        const commentUpdates: CommentUpdate[] = [];

        let utilization: number;
        let load: number;
        let voltage: number;
        let temperature: number;
        let actualCapacity: number;
        let worstBlockConductanceHealth: number;
        if (powerPlant && routineUpdates && plantUpdates && plantUpdates.latestReading && plantRecord && plantConfig) {
            routineUpdates.latestReading = plantUpdates.latestReading;
            const keys = Object.keys(plantUpdates.latestReading);

            const rectifierTypes = plantConfig.rectifierTypes;
            const rectifierPowers: number[] = [];

            if (!rectifierTypes) {
                throw new TypeError('rectifiers are required when adding readings');
            }

            for (const id of rectifierTypes) {
                if (id) {
                    const { power } = await ctx.getStrict('rectifierTypes', id.toString());
                    rectifierPowers.push(power);
                }
            }

            const updateReadingAtDate = (plantRecord: PlantRecord, readingType: string, date: string | Date, value: any) => {
                const comment: CommentUpdate = {} as CommentUpdate;
                comment.readingType = readingType;
                comment.new = value;
                if (keys.includes(readingType)) {
                    comment.manual = true;
                }
                else {
                    comment.manual = false;
                }
                const momentDate = moment(date);
                const isoDate = momentDate.toISOString();
                const isoSeconds = momentDate.valueOf();
                if (plantRecord[readingType]) {
                    const sortedRecord = plantRecord[readingType].sort((a, b) => {
                        const timeA = moment(a[0]).valueOf();
                        const timeB = moment(b[0]).valueOf();
                        if (timeA > timeB) {
                            return -1;
                        }
                        if (timeA < timeB) {
                            return 1;
                        }
                        return 0;
                    });
                    const readingIndex = sortedRecord.findIndex(([iTime]) => moment(iTime).valueOf() <= isoSeconds);
                    if (readingIndex < 0) {
                        throw new Error(`Update Site: no record found of type ${readingType} on ${isoDate}`);
                    }
                    const readingDate = sortedRecord[readingIndex][0];
                    comment.prev = sortedRecord[readingIndex][1];
                    sortedRecord[readingIndex] = [readingDate, value];
                    plantRecord[readingType] =  sortedRecord;

                    if (comment.new !== comment.prev) {
                        commentUpdates.push(comment);
                    }
                } else {
                    throw new Error(`Update Site: no reading type of ${readingType} on PlantRecord`);
                }
            };

            utilization = calcUtilization(plantUpdates.latestReading.load, plantUpdates.latestReading.voltage, rectifierPowers);
            load = plantUpdates.latestReading.load;
            voltage = plantUpdates.latestReading.voltage;
            temperature = plantUpdates.latestReading.temperature;
            actualCapacity = powerPlant.latestReading!.actualCapacity;
            // console.log('powerPlant: ', JSON.stringify(powerPlant));
            worstBlockConductanceHealth = powerPlant.latestReading!.worstBlockConductanceHealth;

            updateReadingAtDate(plantRecord, 'load', routineUpdates.date, plantUpdates.latestReading.load);
            updateReadingAtDate(plantRecord, 'voltage', routineUpdates.date, plantUpdates.latestReading.voltage);
            updateReadingAtDate(plantRecord, 'temperature', routineUpdates.date, plantUpdates.latestReading.temperature);
            updateReadingAtDate(plantRecord, 'utilization', routineUpdates.date, utilization);

            routineUpdates.latestReading.utilization = utilization;
            plantUpdates.latestReading.utilization = utilization;

            routineUpdates.latestReading = Object.assign(routine!.plantReading, routineUpdates.latestReading);
            plantUpdates.latestReading = Object.assign(plantUpdates.latestReading, routine!.plantReading);

        }

        // extract this into a plantConfigOperations function that will contain all plant-config updates.
        if (plantUpdates && plantUpdates.transmission && plantConfig) {
            plantConfig.transmissionConfig = plantUpdates.transmission;
        }
        if (plantUpdates && plantUpdates.serviceLevel && plantConfig) {
            const prevServiceLevel = plantConfig.serviceLevel;
            plantConfig.serviceLevel = plantUpdates.serviceLevel;
        }
        if (plantUpdates && plantUpdates.technologyFlags && plantUpdates.technologyFlags.length && plantConfig) {
            plantConfig.technologyFlags = plantUpdates.technologyFlags;
        }

        if (routineUpdates && powerPlant!.latestReading!.date.getTime() === new Date(routineUpdates!.date).getTime()) {
            const updatedPlant = Object.assign(powerPlant, plantUpdates);
        }
        const message = { commentUpdates };
        return {
            plantUpdates,
            routineUpdates,
            message
        };
    }

    async routineOperations(ctx: OCtx) {
        const {
            routine
        } = ctx.data;

        const {
            routineUpdates
        } = ctx.input;
        if (routine && routineUpdates && routine.plantReading && routineUpdates!.latestReading) {
            routine.plantReading = Object.assign(routine.plantReading, routineUpdates.latestReading);
            routine.editDate = new Date();
            routine.editDate = Object.assign(routine.editDate, new Date());
        }
        const updatedRoutine = Object.assign(routine, routineUpdates);
        return ctx.input;
    }

    async batteryOperations(ctx: OCtx) {
        const {
            batteries
        } = ctx.data;

        const {
            batteryUpdates
        } = ctx.input;

        let snStatusInt: number | undefined;
        if (batteries && batteries.length && batteryUpdates && batteryUpdates.serialNumbers) {
            for (const { batteryId, serialNumber } of batteryUpdates.serialNumbers) {
                const battery = batteries.find((b) => b.id === batteryId);
                if (battery) {
                    battery.serialNumber = serialNumber;
                }
            }
            let batteriesSupplimental: Battery[] = [];
            if (batteryUpdates.batteriesIdsSupplimental && batteryUpdates.batteriesIdsSupplimental.length) {
                for (const battery of batteryUpdates.batteriesIdsSupplimental) {
                    const batt = await ctx.readStrict('batteries', battery);
                    batteriesSupplimental.push(batt);
                }
            }
            const _currentBatteries = [...batteries, ...batteriesSupplimental];
            const sns = getSns(_currentBatteries);
            snStatusInt = getSnStatus(sns);
        }
        const updatedBattery = Object.assign(batteries, batteryUpdates);
        const message = batteryUpdates && batteryUpdates.serialNumbers ? snStatusInt : undefined;
        return {
            message
        };
    }

    async commentOperations (ctx, comments) {
        const { create, input, data } = ctx;
        const submitter = input.submitter;
        const siteId = data.site.id;
        const plantId = data.powerPlant.id;
        let comment: LogItem | undefined;
        if (comments.commentUpdates.length) {
            const date = data.routine.editDate;
            comment = create('logItems', {
                submitter,
                date,
                site: siteId,
                powerPlant: plantId,
                commentUpdates: comments.commentUpdates,
            });
        }
        if (comments.serialNumberUpdate) {
            const date = new Date();
            comment = create('logItems', {
                submitter,
                date,
                site: siteId,
                powerPlant: plantId,
                type: 'serial-number',
                commentUpdates: [{
                    readingType: 'serial-number',
                    new: comments.serialNumberUpdate,
                }],
            });
        }
        return comment;
    }
}
const calculateRoutineCondition = async (
    ctx,
    site,
    date,
    routine,
    plantConfig,
    companyConfig,
    load,
    voltage,
    temperature,
    utilization,
    actualCapacity,
    worstBlockConductanceHealth,
    ) => {
    const siteConfigIds = await ctx.queryIds('siteConfigs', {
        filter: {
          site: site.id
        }
    }) as string[];
    const dateSortedSiteConfigs = (await Promise.all(siteConfigIds.map(async (id) => await ctx.readStrict('siteConfigs', id))))
        .sort(({ date: a}, { date: b}) =>
          a > b ?
            -1 :
          a < b ?
            1 :
            0
        );
    const siteConfig = dateSortedSiteConfigs.find((config) => config.date.valueOf() <= Date.parse(date));
    if (!siteConfig) {
        throw new Error(`No site config found`);
    }

    const powerPlantType = await ctx.readStrict('powerPlantTypes', plantConfig.powerPlantType);
    const referenceVoltage = powerPlantType.voltage;
    const runtime = calcRuntime(
        actualCapacity, referenceVoltage, load, voltage, companyConfig.runtimeDegradationMultiplier
    );
    // console.log('referenceVoltage: ', referenceVoltage);
    // console.log('companyConfig.runtimeDegradationMultiplier: ', companyConfig.runtimeDegradationMultiplier);
    // console.log('voltage: ', voltage);
    // console.log('load: ', load);
    // console.log('runtime: ', runtime);
    // console.log('routine: ', routine);

    let overideInt = 0;
    if (routine.routineUpload) {
    const routineUpload = await ctx.readStrict('routineUploads', routine.routineUpload);
    overideInt = routineUpload.conditionOverride ?
        (
        routineUpload.conditionOverride === 'warn' ?
            1 : 2
        ) :
        0;
    }

    let primaryBatTypeId: string | undefined;
    let primaryBatteryType: BatteryType | undefined;
    if (plantConfig.strings.length || plantConfig.snmpStrings.length) {
        let strings: BatteryString[];
        if (routine.routineType === 'routine') {
            strings = plantConfig.strings
        }
        else if (plantConfig.connectionStatus === 'live') {
            strings = plantConfig.snmpStrings
        }
        else {
            strings = plantConfig.strings
        }
        let batteryIds: string[] = [];
        for (const string of strings) {
            batteryIds = [...batteryIds, ...string.batteries];
        }
        const batteries = (await Promise.all(batteryIds.map(async (id) => await ctx.readStrict('batteries', id))));
        primaryBatTypeId = getBatteryTypeIdByString(strings, batteries);
    }
    if (primaryBatTypeId!) {
        primaryBatteryType = await ctx.getStrict('batteryTypes', primaryBatTypeId!);
    }
    const thermalProbe = plantConfig.thermalProbe;
    let floatVoltageStatus = 0;

    if (
    thermalProbe !== undefined &&
    (
        primaryBatteryType &&
        primaryBatteryType.nominalVPCVoltage !== undefined &&
        primaryBatteryType.compVoltPerCelsius !== undefined
    )
    ) {
    const floatVoltagePerBlock = calcFloatVoltagePerJar(
        temperature,
        thermalProbe,
        primaryBatteryType as {
        nominalVPCVoltage: number;
        compVoltPerCelsius: number;
        voltage: number;
        },
        powerPlantType.model
    );

    const floatVoltageRanges = calcNominalFloatVoltageRanges(
        floatVoltagePerBlock,
        primaryBatteryType.voltage,
        powerPlantType.voltage,
        companyConfig.criticalFloatMod
    );

    floatVoltageStatus = findFloatVoltageStatus(
        floatVoltageRanges,
        voltage,
    );
    }
    const runtimeThresholds = plantConfig.optimalRuntimeThresholdsOverride && !plantConfig.optimalRuntimeThresholdsOverride!.length
        ? findRuntimeThresholds(
            companyConfig.optimalRuntimeFunctionName,
            companyConfig.runtimeThresholdTable,
            {
                hasGenerator: !!siteConfig.generator,
                locationType: site.locationType,
                transmissionConfig: plantConfig.transmissionConfig,
            }
    )   : plantConfig.optimalRuntimeThresholdsOverride;
    const batteryPlantStatus = findRuntimeStatus(
        percisionRound(runtime, companyConfig.runtimePercision),
        runtimeThresholds
    );

    const powerPlantStatus = findUtilizationStatus(
        percisionRound(utilization, companyConfig.utilizationPercision),
        companyConfig.utilizationThresholdTable[0]
    );

    const temperatureStatus = findTempStatus(temperature);

    const condition =  Math.max(
        overideInt,
        batteryPlantStatus,
        powerPlantStatus,
        floatVoltageStatus,
        temperatureStatus
    );

    return condition;
};