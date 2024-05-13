/* 
  app.module.ts
*/

import { Module } from '@nestjs/common';
import { ConfigModule } from '@nestjs/config';
import { MongoModule } from './mongodb/mongo.module';
import { KafkaModule } from './kafka/kafka.module';
import { FinancialFraudModule } from './financial-fraud/financial-fraud.module';
import { FinancialFraudService } from './financial-fraud/financial-fraud.service';

@Module({
  imports: [
    ConfigModule.forRoot(),
    MongoModule,
    KafkaModule,
    FinancialFraudModule,
  ],
  controllers: [],
  providers: [FinancialFraudService],
})
export class AppModule {}
