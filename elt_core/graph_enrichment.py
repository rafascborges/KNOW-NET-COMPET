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


def run_all_enrichments(loader):
    """
    Run all graph enrichment queries.
    
    Args:
        loader: GraphLoader instance with execute_cypher method
    """
    print("Running Graph Enrichment...")
    
    # COMPETED_WITH relationships
    competed_count = create_competed_with(loader)
    print(f"  ✓ Created {competed_count} COMPETED_WITH relationships")
    
    # Future enrichments can be added here:
    # shareholder_count = create_shareholder_of(loader)
    # print(f"  ✓ Created {shareholder_count} SHAREHOLDER_OF relationships")
    
    print("Graph enrichment complete.")

