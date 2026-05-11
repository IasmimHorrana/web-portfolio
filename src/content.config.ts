import { defineCollection } from 'astro:content';
import { glob } from 'astro/loaders';
import { z } from 'astro/zod';

export const collections = {
	projetos: defineCollection({
		// Load Markdown files in the src/content/projetos directory.
		loader: glob({ base: './src/content/projetos', pattern: '**/*.md' }),
		schema: z.object({
			title: z.string(),
			description: z.string().optional(),
			publishDate: z.coerce.date(),
			tags: z.array(z.string()),
			img: z.string().optional(),
			img_alt: z.string().optional(),
			github: z.string().url().optional(),
		}),
	}),
};
