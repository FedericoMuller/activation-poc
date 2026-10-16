import { BadRequestException, Body, Controller, Get, HttpCode, NotFoundException, Param, Post } from '@nestjs/common';
import { SimulateFailure } from '@poc/contracts';
import { ActivationsService } from './activations.service';

export interface CreateActivationDto {
  customerId: string;
  planId: string;
  simulateFailure?: SimulateFailure;
}

const FAILURE_MODES: SimulateFailure[] = ['none', 'billing', 'provisioning'];

@Controller('activations')
export class ActivationsController {
  constructor(private readonly service: ActivationsService) {}

  /** RF-02: responde 202 con el activationId sin esperar a los demás servicios. */
  @Post()
  @HttpCode(202)
  async create(@Body() dto: CreateActivationDto) {
    if (!dto?.customerId?.trim() || !dto?.planId?.trim()) {
      throw new BadRequestException('customerId y planId son obligatorios');
    }
    const mode = dto.simulateFailure ?? 'none';
    if (!FAILURE_MODES.includes(mode)) {
      throw new BadRequestException('simulateFailure debe ser none, billing o provisioning');
    }
    return this.service.create(dto.customerId.trim(), dto.planId.trim(), mode);
  }

  @Get()
  list() {
    return this.service.list();
  }

  /** RF-09: estado actual e historial de eventos. */
  @Get(':id')
  async get(@Param('id') id: string) {
    const doc = await this.service.get(id);
    if (!doc) throw new NotFoundException(`Activación ${id} no encontrada`);
    return doc;
  }
}
