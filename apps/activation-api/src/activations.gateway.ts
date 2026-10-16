import { WebSocketGateway, WebSocketServer } from '@nestjs/websockets';
import { Server } from 'socket.io';
import { TimelineMessage } from '@poc/contracts';

/** RF-08: cada cambio de la activación se empuja a la UI. */
@WebSocketGateway({ cors: { origin: '*' } })
export class ActivationsGateway {
  @WebSocketServer() server: Server;

  broadcast(msg: TimelineMessage) {
    this.server?.emit('activation.event', msg);
  }
}
