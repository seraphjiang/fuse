// SPDX-License-Identifier: Apache-2.0

import { AppMountParameters, CoreSetup, CoreStart, Plugin } from '../../../src/core/public';
import { PLUGIN_ID, PLUGIN_NAME } from '../common';

export class FuseQueryPlugin implements Plugin {
  public setup(core: CoreSetup) {
    core.application.register({
      id: PLUGIN_ID,
      title: PLUGIN_NAME,
      async mount(params: AppMountParameters) {
        const { renderApp } = await import('./application');
        const [coreStart] = await core.getStartServices();
        return renderApp(coreStart, params);
      },
    });
  }

  public start(_core: CoreStart) {}

  public stop() {}
}
