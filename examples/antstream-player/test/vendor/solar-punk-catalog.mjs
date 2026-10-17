// Solar Punk's viewer catalog schema, translated to JavaScript for
// test/catalog-schema.mjs.
//
// Copied from Solar-Punk-Ltd/stream-event-client, branch fix/fmp4-ext-x-map
// at commit 4169fed, src/shared/contracts/catalog.ts and mediaType.ts,
// which copy Solar-Punk-Ltd/streaming-monorepo at c1696c26,
// packages/contracts/src/catalog.ts and mediaType.ts. Only the types are
// gone; refresh it from there when the stream list format changes.
//
// zod is not vendored: the schemas are built from the zod module the caller
// passes (zod 4, for `z.looseObject`).

export function solarPunkCatalog(z) {
  // mediaType.ts
  const MEDIA_TYPE_VIDEO = 'video';
  const MEDIA_TYPE_AUDIO = 'audio';
  const MEDIA_TYPES = [MEDIA_TYPE_VIDEO, MEDIA_TYPE_AUDIO];
  const mediaTypeSchema = z.enum(MEDIA_TYPES);

  // catalog.ts

  /** A rung of an entry's quality ladder as the viewer reads it: every number finite, and fields it does not know kept. */
  const viewerCatalogRungSchema = z.looseObject({
    name: z.string(),
    topic: z.string(),
    width: z.number(),
    height: z.number(),
    bandwidth: z.number(),
    avgBandwidth: z.number(),
    index: z.number().optional(),
    duration: z.number().optional(),
  });

  /**
   * One entry as the viewer reads it. A state it does not know passes, and is shown as not live. The duration may be
   * text, and a scheduled start text or a number, as writers before this contract wrote them.
   */
  const viewerCatalogEntrySchema = z.looseObject({
    owner: z.string(),
    topic: z.string(),
    title: z.string(),
    timestamp: z.number(),
    mediatype: mediaTypeSchema,
    state: z.string().optional(),
    duration: z.union([z.string(), z.number()]).optional(),
    index: z.number().optional(),
    thumbnail: z.string().optional(),
    scheduledStartTime: z.union([z.string(), z.number()]).nullable().optional(),
    renditions: z.array(viewerCatalogRungSchema).optional(),
  });

  /** The whole catalog as the viewer reads it, refused whole when any one entry is refused. */
  const viewerCatalogSchema = z.array(viewerCatalogEntrySchema);

  return { MEDIA_TYPES, mediaTypeSchema, viewerCatalogRungSchema, viewerCatalogEntrySchema, viewerCatalogSchema };
}
