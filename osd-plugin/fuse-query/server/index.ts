// SPDX-License-Identifier: Apache-2.0

import { PluginInitializerContext } from '../../../src/core/server';
import { FuseQueryServerPlugin } from './plugin';

export const plugin = (context: PluginInitializerContext) =>
  new FuseQueryServerPlugin(context);
