import os
import subprocess
import sys


def import_all(nodes, edges, db_name="fraudnetworks"):
    import_command = [
        "neo4j-admin", "import",
        "--database=" + db_name
    ]

    import_command.extend(gather_args("--nodes=Account={0}", nodes))
    import_command.extend(gather_args("--relationships=CONTAINS={0}", edges))

    subprocess.run(import_command)


def gather_args(arg_template, directory):
    return list(map(lambda filepath: arg_template.format(filepath),
                    map(lambda filename: os.path.join(directory, filename), os.listdir(directory))))


if __name__ == "__main__":
    neo4j_directory = sys.argv[1]
    if not os.path.isdir(neo4j_directory):
        sys.exit(1)

    imports_dir = neo4j_directory + "/import/"
    nodes_imports_dir = imports_dir + "nodes.csv/"
    edges_imports_dir = imports_dir + "edges.csv/"

    #shutil.copytree("vertices.csv/", nodes_imports_dir)
    #shutil.copytree("edges.csv/", edges_imports_dir) # Expecting Spark parts results as a directory of csvs

    import_all(nodes_imports_dir, edges_imports_dir)
