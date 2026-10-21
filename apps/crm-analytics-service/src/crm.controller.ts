import { Controller, Get, HttpCode, Post, Query } from '@nestjs/common';
import { CrmService } from './crm.service';

@Controller()
export class CrmController {
  constructor(private readonly crm: CrmService) {}

  /** Todos los eventos registrados; con ?correlationId=act-xxxx, la traza de una activación. */
  @Get('events')
  events(@Query('correlationId') correlationId?: string) {
    return this.crm.list(correlationId);
  }

  @Get('stats')
  stats() {
    return this.crm.stats();
  }

  @Post('replay')
  @HttpCode(200)
  replay() {
    return this.crm.replay();
  }
}
