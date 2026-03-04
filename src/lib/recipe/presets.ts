import { Recipe, RecipeObject, RecipeSettings } from '../types.js';
import { DEFAULT_SETTINGS } from './schema.js';

const CPQ_EXT_ID = 'CPQ_External_ID__c';
const ATG_EXT_ID = 'ATGExternalID__c';

function obj(sobject: string, externalIdField: string | null, extra?: Partial<RecipeObject>): RecipeObject {
  return { sobject, externalIdField, ...extra };
}

// ─── CPQ Full ──────────────────────────────────────────────────────────────

export const CPQ_FULL: Recipe = {
  name: 'CPQ Full Configuration',
  version: '2.0',
  description: 'All CPQ configuration objects for sandbox seeding — products, pricing, rules, templates, approvals, favorites, scripts.',
  objects: [
    // Tier 0 — no dependencies
    obj('SBQQ__BlockPrice__c', ATG_EXT_ID),
    obj('SBQQ__CustomScript__c', ATG_EXT_ID),
    obj('SBQQ__Favorite__c', null),
    obj('SBQQ__InstallProcessorLog__c', null),
    obj('SBQQ__SearchFilter__c', ATG_EXT_ID),
    obj('SBQQ__SummaryVariable__c', CPQ_EXT_ID),
    obj('SBQQ__TemplateContent__c', ATG_EXT_ID),
    obj('Product2', CPQ_EXT_ID, { filter: 'IsActive = true' }),
    obj('Pricebook2', null),

    // Tier 1 — depends on Product2, Pricebook2, SummaryVariable, TemplateContent
    obj('SBQQ__Cost__c', ATG_EXT_ID),
    obj('SBQQ__CustomAction__c', ATG_EXT_ID),
    obj('SBQQ__DiscountSchedule__c', CPQ_EXT_ID),
    obj('SBQQ__ProductFeature__c', CPQ_EXT_ID),
    obj('SBQQ__ProductRule__c', CPQ_EXT_ID, {
      preImport: { deactivate: 'SBQQ__Active__c' },
      postImport: { reactivate: 'SBQQ__Active__c' },
    }),
    obj('SBQQ__PriceRule__c', CPQ_EXT_ID, {
      preImport: { deactivate: 'SBQQ__Active__c' },
      postImport: { reactivate: 'SBQQ__Active__c' },
    }),
    obj('SBQQ__QuoteTemplate__c', CPQ_EXT_ID),
    obj('PricebookEntry', null, {
      filter: 'IsActive = true',
      compositeKey: {
        strategy: 'lookup-match',
        matchFields: [
          { field: 'Product2Id', matchBy: 'Product2.ProductCode' },
          { field: 'Pricebook2Id', matchBy: 'Pricebook2.Name' },
        ],
        additionalMatchFields: ['CurrencyIsoCode'],
      },
    }),

    // Tier 2 — depends on rules, products, features, favorites, templates
    obj('SBQQ__ProductOption__c', CPQ_EXT_ID),
    obj('SBQQ__OptionConstraint__c', ATG_EXT_ID),
    obj('SBQQ__FavoriteProduct__c', null),
    obj('SBQQ__TemplateSection__c', ATG_EXT_ID),
    obj('SBQQ__ErrorCondition__c', ATG_EXT_ID),
    obj('SBQQ__ConfigurationRule__c', ATG_EXT_ID),
    obj('SBQQ__PriceCondition__c', ATG_EXT_ID),
    obj('SBQQ__PriceAction__c', ATG_EXT_ID),
    obj('SBQQ__LookupQuery__c', ATG_EXT_ID),
    obj('SBQQ__ProductAction__c', ATG_EXT_ID),
    obj('SBQQ__CustomActionCondition__c', ATG_EXT_ID),
    obj('SBQQ__DiscountTier__c', ATG_EXT_ID),
    obj('SBQQ__LineColumn__c', ATG_EXT_ID),

    // Tier 3 — remaining templates
    obj('SBQQ__Theme__c', ATG_EXT_ID),

    // Tier 4 — approvals
    obj('sbaa__ApprovalRule__c', CPQ_EXT_ID, {
      preImport: { deactivate: 'sbaa__IsActive__c' },
      postImport: { reactivate: 'sbaa__IsActive__c' },
    }),
    obj('sbaa__ApprovalCondition__c', ATG_EXT_ID),
    obj('sbaa__ApprovalVariable__c', ATG_EXT_ID),
    obj('sbaa__ApprovalChain__c', ATG_EXT_ID),
    obj('sbaa__Approver__c', ATG_EXT_ID),
    obj('sbaa__EmailTemplate__c', ATG_EXT_ID),

    // Tier 5 — custom junctions
    obj('Approval_Rule_Product__c', CPQ_EXT_ID),
    obj('Product_Mapping__c', CPQ_EXT_ID),
  ],
  settings: { ...DEFAULT_SETTINGS },
};

// ─── CPQ Rules Only ────────────────────────────────────────────────────────

export const CPQ_RULES: Recipe = {
  name: 'CPQ Rules',
  version: '1.0',
  description: 'Price Rules + Product Rules and their children (conditions, actions, lookup queries, summary variables).',
  objects: [
    obj('SBQQ__SummaryVariable__c', CPQ_EXT_ID),
    obj('SBQQ__PriceRule__c', CPQ_EXT_ID, {
      preImport: { deactivate: 'SBQQ__Active__c' },
      postImport: { reactivate: 'SBQQ__Active__c' },
    }),
    obj('SBQQ__ProductRule__c', CPQ_EXT_ID, {
      preImport: { deactivate: 'SBQQ__Active__c' },
      postImport: { reactivate: 'SBQQ__Active__c' },
    }),
    obj('SBQQ__PriceCondition__c', ATG_EXT_ID),
    obj('SBQQ__PriceAction__c', ATG_EXT_ID),
    obj('SBQQ__ErrorCondition__c', ATG_EXT_ID),
    obj('SBQQ__ProductAction__c', ATG_EXT_ID),
    obj('SBQQ__ConfigurationRule__c', ATG_EXT_ID),
    obj('SBQQ__LookupQuery__c', ATG_EXT_ID),
  ],
  settings: { ...DEFAULT_SETTINGS },
};

// ─── CPQ Products ──────────────────────────────────────────────────────────

export const CPQ_PRODUCTS: Recipe = {
  name: 'CPQ Products',
  version: '1.0',
  description: 'Product2 + PricebookEntry + ProductFeature + ProductOption + OptionConstraint.',
  objects: [
    obj('Product2', CPQ_EXT_ID, { filter: 'IsActive = true' }),
    obj('Pricebook2', null),
    obj('PricebookEntry', null, {
      filter: 'IsActive = true',
      compositeKey: {
        strategy: 'lookup-match',
        matchFields: [
          { field: 'Product2Id', matchBy: 'Product2.ProductCode' },
          { field: 'Pricebook2Id', matchBy: 'Pricebook2.Name' },
        ],
        additionalMatchFields: ['CurrencyIsoCode'],
      },
    }),
    obj('SBQQ__ProductFeature__c', CPQ_EXT_ID),
    obj('SBQQ__ProductOption__c', CPQ_EXT_ID),
    obj('SBQQ__OptionConstraint__c', ATG_EXT_ID),
  ],
  settings: { ...DEFAULT_SETTINGS },
};

// ─── CPQ Templates ─────────────────────────────────────────────────────────

export const CPQ_TEMPLATES: Recipe = {
  name: 'CPQ Templates',
  version: '1.0',
  description: 'QuoteTemplate + TemplateContent + TemplateSection + LineColumn + Theme.',
  objects: [
    obj('SBQQ__QuoteTemplate__c', CPQ_EXT_ID),
    obj('SBQQ__TemplateContent__c', ATG_EXT_ID),
    obj('SBQQ__TemplateSection__c', ATG_EXT_ID),
    obj('SBQQ__LineColumn__c', ATG_EXT_ID),
    obj('SBQQ__Theme__c', ATG_EXT_ID),
  ],
  settings: { ...DEFAULT_SETTINGS },
};

// ─── Approvals ─────────────────────────────────────────────────────────────

export const APPROVALS: Recipe = {
  name: 'Approvals',
  version: '1.0',
  description: 'ApprovalRule + ApprovalCondition + ApprovalVariable + ApprovalChain + Approver + EmailTemplate + Approval_Rule_Product.',
  objects: [
    obj('Product2', CPQ_EXT_ID, { filter: 'IsActive = true' }),
    obj('sbaa__ApprovalRule__c', CPQ_EXT_ID, {
      preImport: { deactivate: 'sbaa__IsActive__c' },
      postImport: { reactivate: 'sbaa__IsActive__c' },
    }),
    obj('sbaa__ApprovalCondition__c', ATG_EXT_ID),
    obj('sbaa__ApprovalVariable__c', ATG_EXT_ID),
    obj('sbaa__ApprovalChain__c', ATG_EXT_ID),
    obj('sbaa__Approver__c', ATG_EXT_ID),
    obj('sbaa__EmailTemplate__c', ATG_EXT_ID),
    obj('Approval_Rule_Product__c', CPQ_EXT_ID),
  ],
  settings: { ...DEFAULT_SETTINGS },
};

// ─── Preset registry ───────────────────────────────────────────────────────

export const PRESETS: Record<string, Recipe> = {
  'cpq-full': CPQ_FULL,
  'cpq-rules': CPQ_RULES,
  'cpq-products': CPQ_PRODUCTS,
  'cpq-templates': CPQ_TEMPLATES,
  'approvals': APPROVALS,
};

export function listPresets(): { key: string; name: string; objectCount: number; description: string }[] {
  return Object.entries(PRESETS).map(([key, recipe]) => ({
    key,
    name: recipe.name,
    objectCount: recipe.objects.length,
    description: recipe.description,
  }));
}
