use codegen::{Module, Scope};
use convert_case::{Case, Casing};

use super::schema::GenColumn;

enum ColumnInfoTree {
    Leaf(GenColumn),
    Branch(ColumnInfoBranch),
}

struct ColumnInfoBranch(Vec<(String, ColumnInfoTree)>);

pub fn add_column_info_modules(scope: &mut Scope, gen_columns: &[GenColumn]) {
    let module = scope.new_module("columns").vis("pub");
    let sort = module
        .new_enum("SortColumn")
        .vis("pub")
        .derive("Clone")
        .derive("Copy")
        .derive("Debug")
        .derive("Eq")
        .derive("Ord")
        .derive("PartialEq")
        .derive("PartialOrd");

    let mut root = ColumnInfoBranch(vec![]);
    let mut sort_column_names = vec![];

    for gen_column in gen_columns {
        root.add(
            gen_column
                .rust_path
                .iter()
                .map(|(part, _)| part)
                .rev()
                .cloned()
                .collect(),
            gen_column,
        );

        if gen_column.is_sort_column() {
            let name = gen_column.variant_name();
            sort.new_variant(&name);
            sort_column_names.push((name, gen_column.index));
        }
    }

    if !sort_column_names.is_empty() {
        let sort_column_impl = module
            .new_impl("SortColumn")
            .impl_trait("parquetry::SortColumn");

        let sort_column_index = sort_column_impl.new_fn("index").arg_ref_self().ret("usize");

        sort_column_index.line("match self {");

        for (name, index) in sort_column_names {
            sort_column_index.line(format!("Self::{} => {},", name, index));
        }

        sort_column_index.line("}");
    }

    for (name, tree) in root.0 {
        tree.add(module, &name);
    }
}

impl ColumnInfoTree {
    fn add(&self, module: &mut Module, name: &str) {
        match self {
            Self::Leaf(gen_column) => {
                let path_parts = gen_column
                    .descriptor
                    .path()
                    .parts()
                    .iter()
                    .map(|part| format!("\"{}\"", part))
                    .collect::<Vec<_>>()
                    .join(", ");

                let def = format!("pub const {}: parquetry::ColumnInfo = parquetry::ColumnInfo {{ index: {}, path: &[{}] }};", name.to_case(Case::ScreamingSnake), gen_column.index, path_parts);

                module.scope().raw(def);
            }
            Self::Branch(branch) => {
                let child_module = module.new_module(name).vis("pub");
                for (name, tree) in &branch.0 {
                    tree.add(child_module, name);
                }
            }
        }
    }
}

impl ColumnInfoBranch {
    fn get_branch(&mut self, target_name: &str) -> Option<&mut ColumnInfoBranch> {
        self.0.iter_mut().find_map(|(name, tree)| match tree {
            ColumnInfoTree::Branch(branch) if name == target_name => Some(branch),
            _ => None,
        })
    }

    fn add(&mut self, mut path: Vec<String>, gen_column: &GenColumn) {
        if path.len() == 1 {
            self.0
                .push((path[0].clone(), ColumnInfoTree::Leaf(gen_column.clone())));
        } else {
            if let Some(next) = path.pop() {
                let tree = match self.get_branch(&next) {
                    Some(existing) => existing,
                    None => {
                        self.0.push((
                            next.clone(),
                            ColumnInfoTree::Branch(ColumnInfoBranch(vec![])),
                        ));
                        self.get_branch(&next).unwrap()
                    }
                };

                tree.add(path, gen_column)
            }
        }
    }
}
