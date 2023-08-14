-- Execute these scripts to update the old schema and make it compatible with the dataloader

-- Rename tables
ALTER TABLE public.cubeobjects RENAME TO public.medias;
ALTER TABLE public.objecttagrelations RENAME TO public.taggings;

-- Add the tag types to the tagsets table
ALTER TABLE public.tagsets ADD COLUMN tagtype_id integer;
ALTER TABLE public.tagsets ADD CONSTRAINT "FK_tagsests_tag_types_tagtype_id" FOREIGN KEY (tagtype_id) REFERENCES public.tag_types(id) ON DELETE CASCADE;

UPDATE public.tagsets AS ts
SET tagtype_id = (
	SELECT DISTINCT t.tagtype_id
    FROM public.tags t
    WHERE t.tagset_id = ts.id
    LIMIT 1
);


-- Check that the type of tags correspond to the type of the tagset
CREATE OR REPLACE FUNCTION public.check_matching_tagtype()
RETURNS TRIGGER AS $$
BEGIN
    IF (NEW.tagtype_id IS DISTINCT FROM (SELECT tagtype_id FROM public.tagsets WHERE id = NEW.tagset_id)) THEN
        RAISE EXCEPTION 'Tagtype_id does not match the corresponding tagset''s tagtype_id';
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER check_tagtype_matching
BEFORE INSERT OR UPDATE ON public.tags
FOR EACH ROW
EXECUTE FUNCTION public.check_matching_tagtype();

-- Check that the tag belongs the the tagset of the hierachy
CREATE OR REPLACE FUNCTION public.check_nodes_tagset()
RETURNS TRIGGER AS
$$
DECLARE
  tagset_id_for_hierarchy INTEGER;
BEGIN
  IF NOT EXISTS (
    SELECT 1
    FROM public.tags t 
    JOIN (SELECT tagset_id FROM public.hierarchies WHERE id = NEW.hierarchy_id) h
    ON t.tagset_id = h.tagset_id
    WHERE t.id = NEW.tag_id
  ) THEN
    RAISE EXCEPTION 'The tag_id does not belong to the correct tagset_id for this hierarchy.';
  END IF;
  RETURN NEW;
END;
$$
LANGUAGE plpgsql;

CREATE TRIGGER trigger_check_nodes_tagset
BEFORE INSERT OR UPDATE ON public.nodes
FOR EACH ROW
EXECUTE FUNCTION public.check_nodes_tagset();
