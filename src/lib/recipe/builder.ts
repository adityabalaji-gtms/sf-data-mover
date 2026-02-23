import { DependencyGraph, Recipe } from '../types.js';
import { SchemaAnalyzer } from '../schema/analyzer.js';
import { buildEmptyRecipe, addObjectToRecipe, DEFAULT_SETTINGS } from './schema.js';

/**
 * Non-interactive recipe builder.
 * Takes discovered graph + a list of selected objects and produces a Recipe.
 */
export class RecipeBuilder {
  private analyzer = new SchemaAnalyzer();

  buildFromSelection(
    graph: DependencyGraph,
    selectedObjects: string[],
    recipeName: string,
    description: string,
  ): Recipe {
    const recipe = buildEmptyRecipe(recipeName, description);

    for (const sobject of selectedObjects) {
      const node = graph.nodes.get(sobject);
      if (!node) continue;

      const extId = this.analyzer.pickExternalId(node);
      addObjectToRecipe(recipe, sobject, extId);
    }

    return recipe;
  }

  /**
   * Build a recipe from a preset definition (object list + overrides).
   */
  buildFromPreset(
    preset: PresetDefinition,
  ): Recipe {
    return {
      name: preset.name,
      version: '1.0',
      description: preset.description,
      objects: preset.objects,
      settings: { ...DEFAULT_SETTINGS, ...preset.settingsOverrides },
    };
  }
}

export interface PresetDefinition {
  name: string;
  description: string;
  objects: Recipe['objects'];
  settingsOverrides?: Partial<Recipe['settings']>;
}
