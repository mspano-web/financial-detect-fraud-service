/* 
  financial-fraud.module.ts
*/

import { Module } from '@nestjs/common';
import { KafkaModule } from '../kafka/kafka.module';
import { MongoModule } from '../mongodb/mongo.module';
import { FinancialFraudService } from './financial-fraud.service';
import { MongooseModule } from '@nestjs/mongoose';
import { ConfigModule } from '@nestjs/config';
import { Fraud, FraudSchema } from './financial.schema';

@Module({
  imports: [
    ConfigModule,
    MongooseModule.forFeature([{ name: Fraud.name, schema: FraudSchema }]),
    KafkaModule,
    MongoModule,
  ],
  providers: [FinancialFraudService],
  exports: [MongooseModule],
})
export class FinancialFraudModule {}
