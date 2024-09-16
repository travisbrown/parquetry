use parquet::format::SortingColumn;

pub trait SortColumn {
    fn index(&self) -> usize;
}

#[derive(Copy, Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub struct Sort<C> {
    pub column: C,
    pub descending: bool,
    pub nulls_first: bool,
}

impl<C: Copy> Sort<C> {
    pub fn new(column: C) -> Self {
        Self {
            column,
            descending: false,
            nulls_first: false,
        }
    }

    pub fn descending(&self) -> Self {
        Self {
            column: self.column,
            descending: true,
            nulls_first: self.nulls_first,
        }
    }

    pub fn nulls_first(&self) -> Self {
        Self {
            column: self.column,
            descending: self.descending,
            nulls_first: true,
        }
    }

    pub fn sorting_column(&self) -> SortingColumn
    where
        C: SortColumn,
    {
        SortingColumn::new(
            self.column.index() as i32,
            self.descending,
            self.nulls_first,
        )
    }
}

#[derive(Copy, Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub enum SortKey<C> {
    Columns1(Sort<C>),
    Columns2(Sort<C>, Sort<C>),
    Columns3(Sort<C>, Sort<C>, Sort<C>),
    Columns4(Sort<C>, Sort<C>, Sort<C>, Sort<C>),
    Columns5(Sort<C>, Sort<C>, Sort<C>, Sort<C>, Sort<C>),
}

impl<C: Copy> SortKey<C> {
    pub fn columns(&self) -> Vec<Sort<C>> {
        match self {
            Self::Columns1(column_0) => vec![*column_0],
            Self::Columns2(column_0, column_1) => vec![*column_0, *column_1],
            Self::Columns3(column_0, column_1, column_2) => vec![*column_0, *column_1, *column_2],
            Self::Columns4(column_0, column_1, column_2, column_3) => {
                vec![*column_0, *column_1, *column_2, *column_3]
            }
            Self::Columns5(column_0, column_1, column_2, column_3, column_4) => {
                vec![*column_0, *column_1, *column_2, *column_3, *column_4]
            }
        }
    }
}

impl<C: Copy + SortColumn> From<SortKey<C>> for Vec<SortingColumn> {
    fn from(value: SortKey<C>) -> Self {
        value
            .columns()
            .iter()
            .map(|sort| sort.sorting_column())
            .collect()
    }
}
