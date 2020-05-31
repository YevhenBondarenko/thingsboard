///
/// Copyright © 2016-2020 The Thingsboard Authors
///
/// Licensed under the Apache License, Version 2.0 (the "License");
/// you may not use this file except in compliance with the License.
/// You may obtain a copy of the License at
///
///     http://www.apache.org/licenses/LICENSE-2.0
///
/// Unless required by applicable law or agreed to in writing, software
/// distributed under the License is distributed on an "AS IS" BASIS,
/// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
/// See the License for the specific language governing permissions and
/// limitations under the License.
///

import { Component, Inject } from '@angular/core';
import { Store } from '@ngrx/store';
import { AppState } from '@core/core.state';
import { FormBuilder, FormGroup, Validators } from '@angular/forms';
import { Tenant } from '@app/shared/models/tenant.model';
import { ActionNotificationShow } from '@app/core/notification/notification.actions';
import { TranslateService } from '@ngx-translate/core';
import { ContactBasedComponent } from '../../components/entity/contact-based.component';
import { EntityTableConfig } from '@home/models/entity/entities-table-config.models';

@Component({
  selector: 'tb-tenant',
  templateUrl: './tenant.component.html',
  styleUrls: ['./tenant.component.scss']
})
export class TenantComponent extends ContactBasedComponent<Tenant> {

  constructor(protected store: Store<AppState>,
              protected translate: TranslateService,
              @Inject('entity') protected entityValue: Tenant,
              @Inject('entitiesTableConfig') protected entitiesTableConfigValue: EntityTableConfig<Tenant>,
              protected fb: FormBuilder) {
    super(store, fb, entityValue, entitiesTableConfigValue);
  }

  hideDelete() {
    if (this.entitiesTableConfig) {
      return !this.entitiesTableConfig.deleteEnabled(this.entity);
    } else {
      return false;
    }
  }

  buildEntityForm(entity: Tenant): FormGroup {
    return this.fb.group(
      {
        title: [entity ? entity.title : '', [Validators.required]],
        isolatedTbCore: [entity ? entity.isolatedTbCore : false, []],
        isolatedTbRuleEngine: [entity ? entity.isolatedTbRuleEngine : false, []],
        maxNumberOfQueues: [entity ? entity.maxNumberOfQueues : 1, [Validators.required, Validators.min(1)]],
        maxNumberOfPartitionsPerQueue: [entity ? entity.maxNumberOfPartitionsPerQueue : 1, [Validators.required, Validators.min(1)]],
        additionalInfo: this.fb.group(
          {
            description: [entity && entity.additionalInfo ? entity.additionalInfo.description : '']
          }
        )
      }
    );
  }

  updateEntityForm(entity: Tenant) {
    this.entityForm.patchValue({title: entity.title});
    this.entityForm.patchValue({isolatedTbCore: entity.isolatedTbCore});
    this.entityForm.patchValue({isolatedTbRuleEngine: entity.isolatedTbRuleEngine});
    this.entityForm.patchValue({maxNumberOfQueues: entity.maxNumberOfQueues});
    this.entityForm.patchValue({maxNumberOfPartitionsPerQueue: entity.maxNumberOfPartitionsPerQueue});
    this.entityForm.patchValue({additionalInfo: {description: entity.additionalInfo ? entity.additionalInfo.description : ''}});
  }

  showQueueParams(): boolean {
    let isolatedTbRuleEngine: boolean = this.entityForm.get('isolatedTbRuleEngine').value;
    if (isolatedTbRuleEngine) {
      this.entityForm.controls['maxNumberOfQueues'].enable();
      this.entityForm.controls['maxNumberOfPartitionsPerQueue'].enable();
    } else {
      this.entityForm.controls['maxNumberOfQueues'].disable();
      this.entityForm.controls['maxNumberOfPartitionsPerQueue'].disable();
    }
    return isolatedTbRuleEngine;
  }

  updateFormState() {
    if (this.entityForm) {
      if (this.isEditValue) {
        this.entityForm.enable({emitEvent: false});
        if (!this.isAdd) {
          this.entityForm.get('isolatedTbCore').disable({emitEvent: false});
          this.entityForm.get('isolatedTbRuleEngine').disable({emitEvent: false});
          this.entityForm.get('maxNumberOfQueues').disable({emitEvent: false});
          this.entityForm.get('maxNumberOfPartitionsPerQueue').disable({emitEvent: false});
        }
      } else {
        this.entityForm.disable({emitEvent: false});
      }
    }
  }

  onTenantIdCopied(event) {
    this.store.dispatch(new ActionNotificationShow(
      {
        message: this.translate.instant('tenant.idCopiedMessage'),
        type: 'success',
        duration: 750,
        verticalPosition: 'bottom',
        horizontalPosition: 'right'
      }));
  }

}
