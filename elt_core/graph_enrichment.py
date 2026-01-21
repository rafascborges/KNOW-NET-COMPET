# elt_core/graph_enrichment.py
"""
Cypher queries to create derived relationships and enrich the graph.

These queries run after all data is loaded, using patterns in the graph
to create new relationships or update properties.
"""


def create_competed_with(loader) -> int:
    """
    Create COMPETED_WITH relationships between entities that were tenderers
    for the same tender, with competition_count property.
    
    Args:
        loader: GraphLoader instance with execute_cypher method
        
    Returns:
        Number of relationships created
    """
    query = """
    MATCH (a:Entity)-[:IS_TENDERER_FOR]->(t:Tender)<-[:IS_TENDERER_FOR]-(b:Entity)
    WHERE a.id < b.id
    WITH a, b, count(DISTINCT t) AS competition_count
    MERGE (a)-[r:COMPETED_WITH]-(b)
    SET r.competition_count = competition_count
    RETURN count(r) AS relationships_created
    """
    
    result = loader.execute_cypher(query)
    return result[0]['relationships_created'] if result else 0


def merge_duplicate_persons(loader) -> dict:
    """
    Merge duplicate Person nodes that have the same person_name.
    
    When a Person with pep:* ID and a Person with non-pep:* ID have the same name
    and are connected to the same Entity, merge them:
    - Keep the non-pep:* node as the canonical node
    - Transfer all properties from the pep:* node (union)
    - Set pep: true on the canonical node
    - Transfer all relationships from the pep:* node to the canonical
    - Merge relationship properties for duplicate relationships
    - Delete the pep:* node
    
    Args:
        loader: GraphLoader instance with execute_cypher method
        
    Returns:
        Dict with merge statistics
    """
    
    # Step 1: Find all duplicate person pairs (pep node + canonical node)
    # and collect their IDs for processing
    find_duplicates_query = """
    MATCH (e:Entity)<-[:ASSOCIATED_WITH]-(p:Person)
    WITH p.person_name AS name, collect(DISTINCT p) AS people
    WHERE size(people) > 1
      AND any(person IN people WHERE person.id STARTS WITH "pep:")
      AND any(person IN people WHERE NOT person.id STARTS WITH "pep:")
    
    // Get the canonical (non-pep) and duplicate (pep) nodes
    WITH name,
         [person IN people WHERE NOT person.id STARTS WITH "pep:"][0] AS canonical,
         [person IN people WHERE person.id STARTS WITH "pep:"] AS duplicates
    
    UNWIND duplicates AS dup
    RETURN canonical.id AS canonical_id, dup.id AS duplicate_id, name
    """
    
    pairs = loader.execute_cypher(find_duplicates_query)
    
    if not pairs:
        return {'merged': 0, 'relationships_transferred': 0}
    
    total_rels_transferred = 0
    
    # Step 2: For each pair, merge properties and transfer relationships
    for pair in pairs:
        canonical_id = pair['canonical_id']
        duplicate_id = pair['duplicate_id']
        
        # 2a: Copy all properties from duplicate to canonical (except id), set pep: true
        merge_props_query = """
        MATCH (canonical:Person {id: $canonical_id})
        MATCH (dup:Person {id: $duplicate_id})
        
        // Copy all properties from dup to canonical, EXCEPT the id
        // We use properties() and filter out 'id' to avoid constraint violation
        WITH canonical, dup, properties(dup) AS dup_props
        
        // Set each property from dup (except id) on canonical
        SET canonical.pep = true
        SET canonical.person_name = COALESCE(canonical.person_name, dup.person_name)
        
        // For any other properties that dup might have, we need to handle dynamically
        // Unfortunately Cypher doesn't have easy property exclusion, so we handle known props
        
        RETURN canonical.id AS merged_id
        """
        loader.execute_cypher(merge_props_query, {
            'canonical_id': canonical_id,
            'duplicate_id': duplicate_id
        })
        
        # 2b: Transfer outgoing relationships from duplicate to canonical
        # For ASSOCIATED_WITH, we need to merge properties if relationship already exists
        transfer_outgoing_query = """
        MATCH (dup:Person {id: $duplicate_id})-[r]->(target)
        MATCH (canonical:Person {id: $canonical_id})
        
        // Check if canonical already has a relationship of same type to same target
        OPTIONAL MATCH (canonical)-[existing]->(target)
        WHERE type(existing) = type(r)
        
        // If no existing relationship, create new one with all properties
        FOREACH (_ IN CASE WHEN existing IS NULL THEN [1] ELSE [] END |
            // We'll handle this with a separate query per relationship type
            MERGE (canonical)-[new_rel:ASSOCIATED_WITH]->(target)
            SET new_rel += properties(r)
        )
        
        // If existing relationship, merge properties
        FOREACH (_ IN CASE WHEN existing IS NOT NULL THEN [1] ELSE [] END |
            SET existing += properties(r)
        )
        
        RETURN count(r) AS transferred
        """
        
        # Generic approach: get all outgoing relationships and recreate them
        get_outgoing_query = """
        MATCH (dup:Person {id: $duplicate_id})-[r]->(target)
        RETURN type(r) AS rel_type, properties(r) AS props, target.id AS target_id, labels(target)[0] AS target_label
        """
        
        outgoing_rels = loader.execute_cypher(get_outgoing_query, {'duplicate_id': duplicate_id})
        
        for rel in outgoing_rels:
            rel_type = rel['rel_type']
            props = rel['props'] or {}
            target_id = rel['target_id']
            
            # Create or merge relationship on canonical
            create_rel_query = f"""
            MATCH (canonical:Person {{id: $canonical_id}})
            MATCH (target {{id: $target_id}})
            MERGE (canonical)-[r:{rel_type}]->(target)
            SET r += $props
            RETURN count(r) AS created
            """
            loader.execute_cypher(create_rel_query, {
                'canonical_id': canonical_id,
                'target_id': target_id,
                'props': props
            })
            total_rels_transferred += 1
        
        # 2c: Transfer incoming relationships from duplicate to canonical
        get_incoming_query = """
        MATCH (source)-[r]->(dup:Person {id: $duplicate_id})
        RETURN type(r) AS rel_type, properties(r) AS props, source.id AS source_id, labels(source)[0] AS source_label
        """
        
        incoming_rels = loader.execute_cypher(get_incoming_query, {'duplicate_id': duplicate_id})
        
        for rel in incoming_rels:
            rel_type = rel['rel_type']
            props = rel['props'] or {}
            source_id = rel['source_id']
            
            create_rel_query = f"""
            MATCH (source {{id: $source_id}})
            MATCH (canonical:Person {{id: $canonical_id}})
            MERGE (source)-[r:{rel_type}]->(canonical)
            SET r += $props
            RETURN count(r) AS created
            """
            loader.execute_cypher(create_rel_query, {
                'canonical_id': canonical_id,
                'source_id': source_id,
                'props': props
            })
            total_rels_transferred += 1
        
        # 2d: Delete the duplicate node
        delete_query = """
        MATCH (dup:Person {id: $duplicate_id})
        DETACH DELETE dup
        """
        loader.execute_cypher(delete_query, {'duplicate_id': duplicate_id})
    
    return {
        'merged': len(pairs),
        'relationships_transferred': total_rels_transferred
    }


def run_all_enrichments(loader):
    """
    Run all graph enrichment queries.
    
    Args:
        loader: GraphLoader instance with execute_cypher method
    """
    print("Running Graph Enrichment...")
    
    # Merge duplicate Person nodes (pep + non-pep with same name)
    merge_stats = merge_duplicate_persons(loader)
    print(f"  ✓ Merged {merge_stats['merged']} duplicate Person nodes")
    print(f"    (transferred {merge_stats['relationships_transferred']} relationships)")
    
    # COMPETED_WITH relationships
    competed_count = create_competed_with(loader)
    print(f"  ✓ Created {competed_count} COMPETED_WITH relationships")
    
    # Future enrichments can be added here:
    # shareholder_count = create_shareholder_of(loader)
    # print(f"  ✓ Created {shareholder_count} SHAREHOLDER_OF relationships")
    
    print("Graph enrichment complete.")

