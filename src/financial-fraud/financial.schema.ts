/* 
  financial.schema.ts
*/

import { Prop, Schema, SchemaFactory } from '@nestjs/mongoose';
import { Document } from 'mongoose';

export type FraudDocument = Fraud & Document;

@Schema()
export class Fraud {
  @Prop({ required: true })
  credit_card_number: string;

  @Prop({ required: true })
  amount: number;

  @Prop({ required: true })
  destination: string;

  @Prop({ required: true })
  transaction_datetime: Date;

  @Prop({ required: true })
  location: string;

  @Prop({ required: true })
  type: string;

  @Prop({ required: true })
  status: string;

  @Prop({ required: true })
  id: string;
}

export const FraudSchema = SchemaFactory.createForClass(Fraud);
