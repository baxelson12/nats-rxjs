import {
  type ErrorResult,
  connect,
  type ConnectionOptions,
  type Msg,
  type MsgHdrs,
  type NatsConnection,
  type RequestOptions,
  type SubscriptionOptions,
} from "@nats-io/transport-node";
import {
  catchError,
  defer,
  first,
  firstValueFrom,
  from,
  map,
  Observable,
  share,
  Subject,
  switchMap,
  take,
  tap,
  throwError,
  timeout,
} from "rxjs";

/**
 * Configuration options for the NatsRxClient.
 */
export interface NatsRxjsClientConfig {
  /**
   * NATS server URL or array of URLs.
   * Example: 'nats://localhost:4222' or ['nats://server1:4222', 'nats://server2:4222']
   */
  servers: string | string[];
  /**
   * Optional NATS connection options (passed directly to nats.connect).
   */
  connectionOptions?: Partial<ConnectionOptions>;
}

/**
 * Represents a decoded message received from NATS.
 */
export interface DecodedNatsMsg<T = any> {
  subject: string;
  data: T;
  sid: number;
  reply?: string;
  headers?: MsgHdrs;
  /** Reference to the original NATS message */
  originalMsg: Msg;
}

/**
 * A reactive wrapper around the NATS client using Observables.
 */
export class NatsRxjsClient {
  private config: NatsRxjsClientConfig;
  private connection$: Observable<NatsConnection>;
  private connectionError$ = new Subject<ErrorResult>();
  private connectionInstance: NatsConnection | null = null;
  private isConnecting = false;

  /**
   * Creates an instance of NatsRxjsClient
   * @param config Configuration including NATS server URLs.
   */
  constructor(config: NatsRxjsClientConfig) {
    if (!config || !config.servers || config.servers.length === 0) {
      throw new Error("NATS servers configuration is required.");
    }
    this.config = config;

    this.connection$ = defer(() => {
      // Already connected
      if (this.connectionInstance && !this.connectionInstance.isClosed()) {
        return from(Promise.resolve(this.connectionInstance));
      }

      // Connecting now
      if (this.isConnecting) {
        return this.connectionError$.pipe(
          take(1),
          switchMap((err) => throwError(() => err)),
          () => this.connection$,
        );
      }

      // Initialize new connection
      this.isConnecting = true;
      return from(
        connect({
          servers: this.config.servers,
          ...this.config.connectionOptions,
        }),
      ).pipe(
        tap((nc: NatsConnection) => {
          this.isConnecting = false;
          this.connectionInstance = nc;
          this.monitorConnection(nc);
        }),
        catchError((err: ErrorResult) => {
          this.isConnecting = false;
          this.connectionInstance = null;
          this.connectionError$.next(err);
          return throwError(() => err);
        }),
      );
    }).pipe(share({ resetOnRefCountZero: true }));
  }

  /**
   * Monitors the NATS connection for events.
   * @pararm nc The NATS connection instance.
   */
  private async monitorConnection(nc: NatsConnection): Promise<void> {
    if (!nc) {
      return;
    }
    try {
      for await (const status of nc.status()) {
        if (status.type !== "error" && status.type !== "disconnect") {
          return;
        }
        if (!nc.isClosed()) {
          return;
        }

        this.connectionInstance = null;
        this.isConnecting = false;
      }
    } catch (err) {
      console.error(
        "NATS RxJs Client: Connection status monitoring error:",
        err,
      );
      this.connectionInstance = null;
      this.isConnecting = false;
    }
  }

  /**
   * Gets the current NATS connection instance.
   * You should prefer using publish/subscribe instead.
   * @returns A Promise resolving to the NATS Connection
   */
  public getConnection(): Promise<NatsConnection> {
    return firstValueFrom(this.connection$);
  }

  /**
   * Publishes a message to a given NATS subject.
   * @param subject The NATS subject to publish to.
   * @param payload The message payload (Object).
   * @returns An Observable that completes when the message is flushed or errors.
   */
  public publish(subject: string, payload: Object): Observable<void> {
    return this.connection$.pipe(
      first(),
      switchMap((nc) => {
        if (nc.isClosed()) {
          return throwError(
            () =>
              new Error("Attempted to publish on a closed NATS connection."),
          );
        }
        try {
          nc.publish(subject, JSON.stringify({ ...payload }));
          return from(nc.flush()).pipe(
            catchError((err) => throwError(() => err)),
          );
        } catch (err) {
          return throwError(() => err);
        }
      }),
      catchError((err) => {
        console.error(
          `NATS RxJs Client: Failed to publish to ${subject}:`,
          err,
        );
        return throwError(() => err);
      }),
    );
  }

  /**
   * Subscribes to a NATS subject and returns an observable stream of messages.
   * @param subject The NATS subject to subscribe to.
   * @param options Optional NATS subscription options.
   * @returns An Observable emitting messages pertaining to given subject.
   */
  public listen<T>(
    subject: string,
    options?: SubscriptionOptions,
  ): Observable<DecodedNatsMsg<T>> {
    return this.connection$.pipe(
      first(),
      switchMap((nc) => {
        if (nc.isClosed()) {
          return throwError(
            () =>
              new Error(`Attempted to subscribe on a closed NATS connection.`),
          );
        }

        return new Observable<DecodedNatsMsg<T>>((subscriber) => {
          const sub = nc.subscribe(subject, {
            callback: (err, msg) => {
              if (err) {
                subscriber.error(err);
                return;
              }
              subscriber.next({
                subject: msg.subject,
                data: msg.json<T>(),
                sid: msg.sid,
                reply: msg.reply,
                headers: msg.headers,
                originalMsg: msg,
              } as DecodedNatsMsg<T>);
            },
            ...options,
          });
          return () => sub.drain();
        });
      }),
      share({ resetOnRefCountZero: true }),
    );
  }

  /**
   * Makes a NATS request and returns an Observable.
   * @param subject The subject to send the request to.
   * @param payload Optional payload (Object).
   * @param options Optional NATS request options.
   * @returns An Observable emitting the decoded response or erroring on timeout/NATS error.
   */
  public request<TResponse>(
    subject: string,
    payload?: Object,
    options?: RequestOptions,
  ): Observable<DecodedNatsMsg<TResponse>> {
    const defaultTimeout = options?.timeout ?? 5000; // Default to 5s.

    return this.connection$.pipe(
      first(),
      switchMap((nc) => {
        const data = payload ? JSON.stringify(payload) : undefined;
        if (nc.isClosed()) {
          return throwError(
            () =>
              new Error("Attempted to request on a closed NATS connection."),
          );
        }

        return from(nc.request(subject, data, options)).pipe(
          timeout(defaultTimeout),
          map(
            (msg: Msg) =>
              ({
                subject: msg.subject,
                data: msg.json() as TResponse,
                sid: msg.sid,
                headers: msg.headers,
                originalMsg: msg,
              }) as DecodedNatsMsg<TResponse>,
          ),
          catchError((err) => {
            // Err response from server
            if (
              err &&
              typeof err === "object" &&
              "code" in err &&
              err.code === "503"
            ) {
              return throwError(
                () =>
                  new Error(`No responders available for subject: ${subject}.`),
              );
            }
            // Err response from rxjs
            if (
              err &&
              typeof err === "object" &&
              "name" in err &&
              err.name === "TimeoutError"
            ) {
              return throwError(
                () =>
                  new Error(`Request timed out after ${defaultTimeout} ms.`),
              );
            }
            // ?? Other err
            return throwError(() => err);
          }),
          catchError((err) => throwError(() => err)),
        );
      }),
    );
  }

  /**
   * Gracefully clean up NATS connection(s).
   * Drains subscriptions and closes the econnection.
   * @returns An Observable that completes when cleanup is finished.
   */
  public dispose(): Observable<void> {
    return defer(async () => {
      const nc = this.connectionInstance;
      if (!nc || nc.isClosed()) {
        this.connectionInstance = null;
        this.isConnecting = false;
        return;
      }

      try {
        await nc.drain();
      } catch (err) {
        if (!nc.isClosed()) {
          await nc.close().catch((e) => console.error(e));
        }
        throw err;
      } finally {
        this.connectionInstance = null;
        this.isConnecting = false;
      }
    });
  }
}
