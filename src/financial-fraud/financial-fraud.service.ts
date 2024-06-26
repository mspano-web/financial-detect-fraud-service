/* 
  financial-fraud.service.ts
*/

import { Injectable, Inject } from '@nestjs/common';
import { Producer, Consumer } from 'kafkajs';
import { Model } from 'mongoose';
import { InjectModel } from '@nestjs/mongoose';
import { Fraud } from './financial.schema';
import { Observable, EMPTY, lastValueFrom, EmptyError } from 'rxjs';
import { catchError, defaultIfEmpty } from 'rxjs/operators';
import { TransactionStatus } from '../types/results';
import { KafkaTopics } from '../types/topics';

@Injectable()
export class FinancialFraudService {
  constructor(
    @Inject('PRODUCER_PROVIDER') private readonly kafkaProducer: Producer,
    @Inject('CONSUMER_PROVIDER') private readonly kafkaConsumer: Consumer,
    @InjectModel(Fraud.name)
    private readonly financialDataLogModel: Model<Fraud>,
  ) {}

  // ------------------------------------------------

  onModuleInit() {
    this.startConsumer();
  }

  // ------------------------------------------------

  onModuleDestroy() {
    this.stopConsumer();
  }

  // ------------------------------------------------

  private startConsumer() {
    // The Kafka consumer starts and starts listening for messages on the specified topics
    this.kafkaConsumer.run({
      // The object with an each Message attribute defines a callback function that is executed
      //    every time a message is received on one of the topics to which the consumer is subscribed.
      eachMessage: async ({ topic, message }) => {
        switch (topic) {
          case KafkaTopics.FMS_SUCCESS:
            // subscribe() is used to subscribe to this observable and handle the events emitted by it,
            //    relative to handleTransaction
            this.handleSuccess(message.value.toString()).subscribe();
            break;
          case KafkaTopics.FMS_MOVEMENTS_REPLAY:
            this.handleMovementsReplay(message.value.toString()).subscribe();
            break;
          default:
            console.log('Received message from unknown topic:', topic);
            break;
        }
      },
    });
  }

  // --------------------------------------

  // handleSuccess is responsible for processing incoming transactions.
  // It asynchronously requests information on the historical movements associated with the present transaction,
  //    and handles some error situations.

  handleSuccess(transaction: string): Observable<void> {
    const transactionMessage = JSON.parse(transaction);
    console.log('--------------------------------------------');
    console.log('handleSuccess - transactionMessage: ', transactionMessage);

    return new Observable<void>((observer) => {
      this.sendMessage(KafkaTopics.FMS_MOVEMENTS, transactionMessage).subscribe(
        {
          next: () => {
            console.log(
              'Message sent successfully - topic: ',
              KafkaTopics.FMS_MOVEMENTS,
              'transaction id: ',
              transactionMessage.id,
            );
            observer.complete();
          },
          error: (error) => {
            console.error(
              'Error sending message to: ',
              KafkaTopics.FMS_MOVEMENTS,
              '- transaction id: :',
              transactionMessage.id,
              ' error:',
              error,
            );
            let resultTmp = TransactionStatus.FAILED;
            this.sendMessage(
              KafkaTopics.FMS_COMPENSATION,
              transactionMessage,
            ).subscribe({
              complete: () => {
                console.log(
                  'Requested compensation OK - transaction id:',
                  transactionMessage.id,
                );
              },
              error: (compensationError) => {
                console.error(
                  'Error sending message to: ',
                  KafkaTopics.FMS_COMPENSATION,
                  ' - transaction id:',
                  transactionMessage.id,
                  ' error: ',
                  compensationError,
                );
                resultTmp = TransactionStatus.FAILED_INCONSISTENCE;
              },
            });
            const errorObj = {
              result: resultTmp,
              transaction: transactionMessage,
              error: 'Failure to analyze fraud',
            };
            this.sendMessage(
              KafkaTopics.END_TRANSACTIONS_CREDIT_CARD,
              errorObj,
            ).subscribe({
              complete: () => {
                console.log(
                  'Message sent successfully - topic: ',
                  KafkaTopics.END_TRANSACTIONS_CREDIT_CARD,
                  'transaction id: ',
                  transactionMessage.id,
                );
              },
              error: (endError) => {
                console.error(
                  'Failure when trying to update transaction status - transaction id:',
                  transactionMessage.id,
                  ' error: ',
                  endError,
                );
                throw new Error(endError);
              },
            });
          },
        },
      );
    }).pipe(
      catchError((error) => {
        console.error(
          'Failure to obtain historical information transaction id:',
          transactionMessage.id,
          ' error: ',
          error,
        );
        return EMPTY;
      }),
    );
  }

  // ----------------------------------------------------

  private sendMessage(topic: string, message: any): Observable<void> {
    // Start - Test case compensation -------------------
    if (
      topic === KafkaTopics.FMS_MOVEMENTS &&
      message.credit_card_number &&
      (message.credit_card_number === '2222222222' ||
        message.credit_card_number === '4444444444')
    ) {
      console.log('Test Case - Force Fail & Compensation workflow');
      return new Observable<void>((observer) => {
        observer.error('Test Case - Force Fail & Compensation workflow');
      });
    }
    // End - Test case compensation -------------------

    return new Observable<void>((observer) => {
      this.kafkaProducer
        .send({
          topic,
          messages: [{ value: JSON.stringify(message) }],
        })
        .then(() => {
          observer.complete();
        })
        .catch((error) => {
          console.error(`Failed to send message to ${topic}:`, error);
          observer.error(error);
        });
    });
  }

  // ----------------------------------------------------

  //handleMovementsReplay has the responsibility of processing the Fraud business logic.
  // It starts from receiving information on the associated historical movements asynchronously,
  //    registers it in the database if a suspicious case is detected, and handles some error situations.

  private handleMovementsReplay(message: string): Observable<void> {
    let session: any;
    const data = JSON.parse(message);
    console.log('--------------------------------------------');
    console.log(' handleMovementsReplay - data: ', data);
    const { result, transaction, movements } = data;
    let errorType = TransactionStatus.FAILED;

    return new Observable<void>((observer) => {
      this.financialDataLogModel.db
        .startSession()
        .then(async (sessionInstance: any) => {
          session = sessionInstance;
          await session.startTransaction();

          if (result === 'OK') {
            try {
              if (movements.length > 0) {
                const financialDataLog = new this.financialDataLogModel(
                  transaction,
                );
                await financialDataLog.save();
              }
              // Start - Test case compensation -------------------
              if (
                transaction.credit_card_number &&
                transaction.credit_card_number === '3333333333'
              ) {
                console.log(
                  'Test Case - Force Fail & Compensation  - workflow - transaction id:',
                  transaction.id,
                );
                throw new Error(
                  'Test Case - Force Fail & Compensation  - workflow',
                );
              }
              // End - Test case compensation -------------------

              const message = {
                result: TransactionStatus.OK,
                transaction: transaction,
                error: '',
              };
              console.log('--------------------------------------------');
              console.log(
                ' handleMovementsReplay - response to topic: ',
                KafkaTopics.END_TRANSACTIONS_CREDIT_CARD,
                ' message: ',
                message,
              );
              await lastValueFrom(
                this.sendMessage(
                  KafkaTopics.END_TRANSACTIONS_CREDIT_CARD,
                  message,
                ).pipe(defaultIfEmpty(null)),
              ).catch((error) => {
                if (error instanceof EmptyError) {
                  console.error('The event stream is empty.');
                } else {
                  console.error('Error processing transaction:', error);
                }
                throw error;
              });

              await session.commitTransaction();
              observer.complete();
            } catch (error) {
              const messageError = `Fail in handleMovementsReplay- transaction id: ${transaction.id}  error :${error}`;
              console.log(messageError);
              throw new Error(error);
            }
          } else {
            const messageError = `Internal Error in handleMovementsReplay- transaction id: ${transaction.id} 'result :${result}`;
            console.log(messageError);
            throw new Error(messageError);
          }
        })
        .catch(async (error) => {
          console.error(
            'Error during transaction transaction id:',
            transaction.id,
            ' error: ',
            error,
          );
          try {
            await lastValueFrom(
              this.sendMessage(KafkaTopics.FMS_COMPENSATION, transaction).pipe(
                defaultIfEmpty(null),
              ),
            ).catch((error) => {
              if (error instanceof EmptyError) {
                console.error('The event stream is empty.');
              } else {
                console.error('Error processing transaction:', error);
              }
              throw error;
            });
          } catch {
            console.log(
              'Failure to send message in fraud service to: ',
              KafkaTopics.FMS_COMPENSATION,
            );
            errorType = TransactionStatus.FAILED_INCONSISTENCE;
          }

          try {
            const errorObj = {
              result: errorType,
              transaction: transaction,
              error: 'Failure to process transaction in fraud service',
            };

            await lastValueFrom(
              this.sendMessage(
                KafkaTopics.END_TRANSACTIONS_CREDIT_CARD,
                errorObj,
              ).pipe(defaultIfEmpty(null)),
            ).catch((error) => {
              if (error instanceof EmptyError) {
                console.error('The event stream is empty.');
              } else {
                console.error('Error processing transaction:', error);
              }
              throw error;
            });
          } catch {
            console.log(
              'Failure to send message in fraud service to topic: ',
              KafkaTopics.END_TRANSACTIONS_CREDIT_CARD,
            );
          }

          if (session) {
            if (session.inTransaction()) {
              await session.abortTransaction();
            }
            await session.endSession();
          }
        });
    }).pipe(
      catchError((error) => {
        console.error(
          'Failure to process fraud transaction  in handleMovementsReplay -  transaction id:',
          transaction.id,
          ' error: ',
          error,
        );
        return EMPTY;
      }),
    );
  }

  // --------------------------------------

  private async stopConsumer() {
    await this.kafkaConsumer.disconnect();
  }
}
