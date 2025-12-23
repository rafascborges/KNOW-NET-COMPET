from linkml_runtime.utils.schemaview import SchemaView
import click

@click.command()
@click.argument('schema_path')
def generate_cypher(schema_path):
    """
    Generates Neo4j Cypher constraints from a LinkML schema.
    """
    view = SchemaView(schema_path)
    
    print(f"// Generated Cypher constraints for {view.schema.name}\n")

    for class_name, class_def in view.all_classes().items():
        # Skip mixins and abstract classes if you don't want constraints for them
        if class_def.mixin or class_def.abstract:
            continue

        # 1. GENERATE UNIQUE CONSTRAINTS (for identifiers)
        # In LinkML, the 'identifier: true' slot is the primary key
        identifier_slot = view.get_identifier_slot(class_name)
        if identifier_slot:
            # Cypher syntax: CREATE CONSTRAINT FOR (n:Label) REQUIRE n.property IS UNIQUE
            print(f"// Unique constraint for class: {class_name}")
            print(f"CREATE CONSTRAINT {class_name}_id_unique IF NOT EXISTS")
            print(f"FOR (n:`{class_name}`)")
            print(f"REQUIRE n.`{identifier_slot.name}` IS UNIQUE;\n")

if __name__ == '__main__':
    generate_cypher()