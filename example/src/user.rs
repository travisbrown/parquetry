#![cfg_attr(rustfmt, rustfmt_skip)]
const SCHEMA_SOURCE: &str = "message user {
    required int64 id (integer(64, false));
    required int64 ts (timestamp(millis, true));
    optional int32 status;

    optional group user_info {
        required byte_array screen_name (string);

        optional group user_name_info {
            required byte_array name (string);

            optional group user_profile_info {
                required int64 created_at (timestamp(millis, true));
                optional int32 created_at_date (date);
                required byte_array location (string);
                required byte_array description (string);
                optional byte_array url (string);

                required int32 followers_count;
                required int32 friends_count;
                required int32 favourites_count;
                required int32 statuses_count;

                optional group withheld_in_countries (list) {
                    repeated group list {
                        required byte_array element (string);
                    }
                }
            }
        }
    }
}";
pub static SCHEMA: std::sync::LazyLock<parquet::schema::types::SchemaDescPtr> = std::sync::LazyLock::new(||
std::sync::Arc::new(
    parquet::schema::types::SchemaDescriptor::new(
        std::sync::Arc::new(
            parquet::schema::parser::parse_message_type(SCHEMA_SOURCE).unwrap(),
        ),
    ),
));
#[derive(Clone, Debug, Eq, PartialEq, serde::Deserialize, serde::Serialize)]
pub struct User {
    pub id: u64,
    #[serde(with = "chrono::serde::ts_milliseconds")]
    pub ts: chrono::DateTime<chrono::Utc>,
    pub status: Option<i32>,
    pub user_info: Option<UserInfo>,
}
#[derive(Clone, Debug, Eq, PartialEq, serde::Deserialize, serde::Serialize)]
pub struct UserInfo {
    pub screen_name: String,
    pub user_name_info: Option<UserNameInfo>,
}
#[derive(Clone, Debug, Eq, PartialEq, serde::Deserialize, serde::Serialize)]
pub struct UserNameInfo {
    pub name: String,
    pub user_profile_info: Option<UserProfileInfo>,
}
#[derive(Clone, Debug, Eq, PartialEq, serde::Deserialize, serde::Serialize)]
pub struct UserProfileInfo {
    #[serde(with = "chrono::serde::ts_milliseconds")]
    pub created_at: chrono::DateTime<chrono::Utc>,
    pub created_at_date: Option<chrono::NaiveDate>,
    pub location: String,
    pub description: String,
    pub url: Option<String>,
    pub followers_count: i32,
    pub friends_count: i32,
    pub favourites_count: i32,
    pub statuses_count: i32,
    pub withheld_in_countries: Option<Vec<String>>,
}
pub mod columns {
    #[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
    pub enum SortColumn {
        Id,
        Ts,
        Status,
        ScreenName,
        Name,
        CreatedAt,
        CreatedAtDate,
        Location,
        Description,
        Url,
        FollowersCount,
        FriendsCount,
        FavouritesCount,
        StatusesCount,
    }
    impl parquetry::sort::SortColumn for SortColumn {
        fn index(&self) -> usize {
            match self {
                Self::Id => 0,
                Self::Ts => 1,
                Self::Status => 2,
                Self::ScreenName => 3,
                Self::Name => 4,
                Self::CreatedAt => 5,
                Self::CreatedAtDate => 6,
                Self::Location => 7,
                Self::Description => 8,
                Self::Url => 9,
                Self::FollowersCount => 10,
                Self::FriendsCount => 11,
                Self::FavouritesCount => 12,
                Self::StatusesCount => 13,
            }
        }
    }
    pub const ID: parquetry::ColumnInfo = parquetry::ColumnInfo {
        index: 0,
        path: &["id"],
    };
    pub const TS: parquetry::ColumnInfo = parquetry::ColumnInfo {
        index: 1,
        path: &["ts"],
    };
    pub const STATUS: parquetry::ColumnInfo = parquetry::ColumnInfo {
        index: 2,
        path: &["status"],
    };
    pub mod user_info {
        pub const SCREEN_NAME: parquetry::ColumnInfo = parquetry::ColumnInfo {
            index: 3,
            path: &["user_info", "screen_name"],
        };
        pub mod user_name_info {
            pub const NAME: parquetry::ColumnInfo = parquetry::ColumnInfo {
                index: 4,
                path: &["user_info", "user_name_info", "name"],
            };
            pub mod user_profile_info {
                pub const CREATED_AT: parquetry::ColumnInfo = parquetry::ColumnInfo {
                    index: 5,
                    path: &[
                        "user_info",
                        "user_name_info",
                        "user_profile_info",
                        "created_at",
                    ],
                };
                pub const CREATED_AT_DATE: parquetry::ColumnInfo = parquetry::ColumnInfo {
                    index: 6,
                    path: &[
                        "user_info",
                        "user_name_info",
                        "user_profile_info",
                        "created_at_date",
                    ],
                };
                pub const LOCATION: parquetry::ColumnInfo = parquetry::ColumnInfo {
                    index: 7,
                    path: &[
                        "user_info",
                        "user_name_info",
                        "user_profile_info",
                        "location",
                    ],
                };
                pub const DESCRIPTION: parquetry::ColumnInfo = parquetry::ColumnInfo {
                    index: 8,
                    path: &[
                        "user_info",
                        "user_name_info",
                        "user_profile_info",
                        "description",
                    ],
                };
                pub const URL: parquetry::ColumnInfo = parquetry::ColumnInfo {
                    index: 9,
                    path: &["user_info", "user_name_info", "user_profile_info", "url"],
                };
                pub const FOLLOWERS_COUNT: parquetry::ColumnInfo = parquetry::ColumnInfo {
                    index: 10,
                    path: &[
                        "user_info",
                        "user_name_info",
                        "user_profile_info",
                        "followers_count",
                    ],
                };
                pub const FRIENDS_COUNT: parquetry::ColumnInfo = parquetry::ColumnInfo {
                    index: 11,
                    path: &[
                        "user_info",
                        "user_name_info",
                        "user_profile_info",
                        "friends_count",
                    ],
                };
                pub const FAVOURITES_COUNT: parquetry::ColumnInfo = parquetry::ColumnInfo {
                    index: 12,
                    path: &[
                        "user_info",
                        "user_name_info",
                        "user_profile_info",
                        "favourites_count",
                    ],
                };
                pub const STATUSES_COUNT: parquetry::ColumnInfo = parquetry::ColumnInfo {
                    index: 13,
                    path: &[
                        "user_info",
                        "user_name_info",
                        "user_profile_info",
                        "statuses_count",
                    ],
                };
                pub const WITHHELD_IN_COUNTRIES: parquetry::ColumnInfo = parquetry::ColumnInfo {
                    index: 14,
                    path: &[
                        "user_info",
                        "user_name_info",
                        "user_profile_info",
                        "withheld_in_countries",
                        "list",
                        "element",
                    ],
                };
            }
        }
    }
}
impl parquetry::Schema for User {
    type SortColumn = columns::SortColumn;
    type Writer<W: std::io::Write + Send> = UserWriter<W>;
    fn sort_key_value(
        &self,
        sort_key: parquetry::sort::SortKey<Self::SortColumn>,
    ) -> Vec<u8> {
        {
            let mut bytes = vec![];
            for column in sort_key.columns() {
                self.write_sort_key_bytes(column, &mut bytes);
            }
            bytes
        }
    }
    fn source() -> &'static str {
        SCHEMA_SOURCE
    }
    fn schema() -> parquet::schema::types::SchemaDescPtr {
        SCHEMA.clone()
    }
    fn writer<W: std::io::Write + Send>(
        writer: W,
        properties: parquet::file::properties::WriterProperties,
    ) -> Result<Self::Writer<W>, parquetry::error::Error> {
        {
            Ok(Self::Writer {
                writer: parquet::file::writer::SerializedFileWriter::new(
                    writer,
                    SCHEMA.root_schema_ptr(),
                    std::sync::Arc::new(properties),
                )?,
                workspace: Default::default(),
            })
        }
    }
}
pub struct UserWriter<W: std::io::Write> {
    writer: parquet::file::writer::SerializedFileWriter<W>,
    workspace: ParquetryWorkspace,
}
impl<W: std::io::Write + Send> parquetry::write::SchemaWrite<User, W> for UserWriter<W> {
    fn write_row_group<
        'a,
        E: From<parquetry::error::Error>,
        I: Iterator<Item = Result<&'a User, E>>,
    >(
        &mut self,
        values: &mut I,
    ) -> Result<parquet::file::metadata::RowGroupMetaDataPtr, E>
    where
        User: 'a,
    {
        {
            User::fill_workspace(&mut self.workspace, values)?;
            User::write_with_workspace(&mut self.writer, &mut self.workspace)
                .map_err(E::from)
        }
    }
    fn write_item(&mut self, value: &User) -> Result<(), parquetry::error::Error> {
        User::add_item_to_workspace(&mut self.workspace, value)
    }
    fn finish_row_group(
        &mut self,
    ) -> Result<parquet::file::metadata::RowGroupMetaDataPtr, parquetry::error::Error> {
        User::write_with_workspace(&mut self.writer, &mut self.workspace)
    }
    fn finish(self) -> Result<parquet::format::FileMetaData, parquetry::error::Error> {
        Ok(self.writer.close()?)
    }
}
impl TryFrom<parquet::record::Row> for User {
    type Error = parquetry::error::Error;
    fn try_from(row: parquet::record::Row) -> Result<Self, parquetry::error::Error> {
        {
            let mut fields = row.get_column_iter();
            let id = match fields
                .next()
                .ok_or_else(|| parquetry::error::Error::InvalidField("id".to_string()))?
                .1
            {
                parquet::record::Field::ULong(value) => Ok(*value),
                _ => Err(parquetry::error::Error::InvalidField("id".to_string())),
            }?;
            let ts = match fields
                .next()
                .ok_or_else(|| parquetry::error::Error::InvalidField("ts".to_string()))?
                .1
            {
                parquet::record::Field::TimestampMillis(value) => {
                    Ok(
                        chrono::TimeZone::timestamp_millis_opt(&chrono::Utc, *value)
                            .single()
                            .ok_or_else(|| parquetry::error::Error::InvalidField(
                                "ts".to_string(),
                            ))?,
                    )
                }
                _ => Err(parquetry::error::Error::InvalidField("ts".to_string())),
            }?;
            let status = match fields
                .next()
                .ok_or_else(|| parquetry::error::Error::InvalidField(
                    "status".to_string(),
                ))?
                .1
            {
                parquet::record::Field::Null => Ok(None),
                parquet::record::Field::Int(value) => Ok(Some(*value)),
                _ => Err(parquetry::error::Error::InvalidField("status".to_string())),
            }?;
            let user_info = match fields
                .next()
                .ok_or_else(|| parquetry::error::Error::InvalidField(
                    "user_info".to_string(),
                ))?
                .1
            {
                parquet::record::Field::Null => Ok(None),
                parquet::record::Field::Group(row) => {
                    let mut fields = row.get_column_iter();
                    let screen_name = match fields
                        .next()
                        .ok_or_else(|| parquetry::error::Error::InvalidField(
                            "screen_name".to_string(),
                        ))?
                        .1
                    {
                        parquet::record::Field::Str(value) => Ok(value.clone()),
                        _ => {
                            Err(
                                parquetry::error::Error::InvalidField(
                                    "screen_name".to_string(),
                                ),
                            )
                        }
                    }?;
                    let user_name_info = match fields
                        .next()
                        .ok_or_else(|| parquetry::error::Error::InvalidField(
                            "user_name_info".to_string(),
                        ))?
                        .1
                    {
                        parquet::record::Field::Null => Ok(None),
                        parquet::record::Field::Group(row) => {
                            let mut fields = row.get_column_iter();
                            let name = match fields
                                .next()
                                .ok_or_else(|| parquetry::error::Error::InvalidField(
                                    "name".to_string(),
                                ))?
                                .1
                            {
                                parquet::record::Field::Str(value) => Ok(value.clone()),
                                _ => {
                                    Err(
                                        parquetry::error::Error::InvalidField("name".to_string()),
                                    )
                                }
                            }?;
                            let user_profile_info = match fields
                                .next()
                                .ok_or_else(|| parquetry::error::Error::InvalidField(
                                    "user_profile_info".to_string(),
                                ))?
                                .1
                            {
                                parquet::record::Field::Null => Ok(None),
                                parquet::record::Field::Group(row) => {
                                    let mut fields = row.get_column_iter();
                                    let created_at = match fields
                                        .next()
                                        .ok_or_else(|| parquetry::error::Error::InvalidField(
                                            "created_at".to_string(),
                                        ))?
                                        .1
                                    {
                                        parquet::record::Field::TimestampMillis(value) => {
                                            Ok(
                                                chrono::TimeZone::timestamp_millis_opt(&chrono::Utc, *value)
                                                    .single()
                                                    .ok_or_else(|| parquetry::error::Error::InvalidField(
                                                        "created_at".to_string(),
                                                    ))?,
                                            )
                                        }
                                        _ => {
                                            Err(
                                                parquetry::error::Error::InvalidField(
                                                    "created_at".to_string(),
                                                ),
                                            )
                                        }
                                    }?;
                                    let created_at_date = match fields
                                        .next()
                                        .ok_or_else(|| parquetry::error::Error::InvalidField(
                                            "created_at_date".to_string(),
                                        ))?
                                        .1
                                    {
                                        parquet::record::Field::Null => Ok(None),
                                        parquet::record::Field::Date(value) => {
                                            Ok(
                                                Some(
                                                    chrono::TimeDelta::try_days(*value as i64)
                                                        .and_then(|delta| {
                                                            chrono::NaiveDate::from_ymd_opt(1970, 1, 1)
                                                                .unwrap()
                                                                .checked_add_signed(delta)
                                                        })
                                                        .ok_or_else(|| parquetry::error::Error::InvalidField(
                                                            "created_at_date".to_string(),
                                                        ))?,
                                                ),
                                            )
                                        }
                                        _ => {
                                            Err(
                                                parquetry::error::Error::InvalidField(
                                                    "created_at_date".to_string(),
                                                ),
                                            )
                                        }
                                    }?;
                                    let location = match fields
                                        .next()
                                        .ok_or_else(|| parquetry::error::Error::InvalidField(
                                            "location".to_string(),
                                        ))?
                                        .1
                                    {
                                        parquet::record::Field::Str(value) => Ok(value.clone()),
                                        _ => {
                                            Err(
                                                parquetry::error::Error::InvalidField(
                                                    "location".to_string(),
                                                ),
                                            )
                                        }
                                    }?;
                                    let description = match fields
                                        .next()
                                        .ok_or_else(|| parquetry::error::Error::InvalidField(
                                            "description".to_string(),
                                        ))?
                                        .1
                                    {
                                        parquet::record::Field::Str(value) => Ok(value.clone()),
                                        _ => {
                                            Err(
                                                parquetry::error::Error::InvalidField(
                                                    "description".to_string(),
                                                ),
                                            )
                                        }
                                    }?;
                                    let url = match fields
                                        .next()
                                        .ok_or_else(|| parquetry::error::Error::InvalidField(
                                            "url".to_string(),
                                        ))?
                                        .1
                                    {
                                        parquet::record::Field::Null => Ok(None),
                                        parquet::record::Field::Str(value) => {
                                            Ok(Some(value.clone()))
                                        }
                                        _ => {
                                            Err(
                                                parquetry::error::Error::InvalidField("url".to_string()),
                                            )
                                        }
                                    }?;
                                    let followers_count = match fields
                                        .next()
                                        .ok_or_else(|| parquetry::error::Error::InvalidField(
                                            "followers_count".to_string(),
                                        ))?
                                        .1
                                    {
                                        parquet::record::Field::Int(value) => Ok(*value),
                                        _ => {
                                            Err(
                                                parquetry::error::Error::InvalidField(
                                                    "followers_count".to_string(),
                                                ),
                                            )
                                        }
                                    }?;
                                    let friends_count = match fields
                                        .next()
                                        .ok_or_else(|| parquetry::error::Error::InvalidField(
                                            "friends_count".to_string(),
                                        ))?
                                        .1
                                    {
                                        parquet::record::Field::Int(value) => Ok(*value),
                                        _ => {
                                            Err(
                                                parquetry::error::Error::InvalidField(
                                                    "friends_count".to_string(),
                                                ),
                                            )
                                        }
                                    }?;
                                    let favourites_count = match fields
                                        .next()
                                        .ok_or_else(|| parquetry::error::Error::InvalidField(
                                            "favourites_count".to_string(),
                                        ))?
                                        .1
                                    {
                                        parquet::record::Field::Int(value) => Ok(*value),
                                        _ => {
                                            Err(
                                                parquetry::error::Error::InvalidField(
                                                    "favourites_count".to_string(),
                                                ),
                                            )
                                        }
                                    }?;
                                    let statuses_count = match fields
                                        .next()
                                        .ok_or_else(|| parquetry::error::Error::InvalidField(
                                            "statuses_count".to_string(),
                                        ))?
                                        .1
                                    {
                                        parquet::record::Field::Int(value) => Ok(*value),
                                        _ => {
                                            Err(
                                                parquetry::error::Error::InvalidField(
                                                    "statuses_count".to_string(),
                                                ),
                                            )
                                        }
                                    }?;
                                    let withheld_in_countries = match fields
                                        .next()
                                        .ok_or_else(|| parquetry::error::Error::InvalidField(
                                            "withheld_in_countries".to_string(),
                                        ))?
                                        .1
                                    {
                                        parquet::record::Field::Null => Ok(None),
                                        parquet::record::Field::ListInternal(fields) => {
                                            let mut values = Vec::with_capacity(fields.len());
                                            for field in fields.elements() {
                                                let value = match field {
                                                    parquet::record::Field::Str(value) => Ok(value.clone()),
                                                    _ => {
                                                        Err(
                                                            parquetry::error::Error::InvalidField(
                                                                "withheld_in_countries".to_string(),
                                                            ),
                                                        )
                                                    }
                                                }?;
                                                values.push(value);
                                            }
                                            Ok(Some(values))
                                        }
                                        _ => {
                                            Err(
                                                parquetry::error::Error::InvalidField(
                                                    "withheld_in_countries".to_string(),
                                                ),
                                            )
                                        }
                                    }?;
                                    Ok(
                                        Some(UserProfileInfo {
                                            created_at,
                                            created_at_date,
                                            location,
                                            description,
                                            url,
                                            followers_count,
                                            friends_count,
                                            favourites_count,
                                            statuses_count,
                                            withheld_in_countries,
                                        }),
                                    )
                                }
                                _ => {
                                    Err(
                                        parquetry::error::Error::InvalidField(
                                            "user_profile_info".to_string(),
                                        ),
                                    )
                                }
                            }?;
                            Ok(
                                Some(UserNameInfo {
                                    name,
                                    user_profile_info,
                                }),
                            )
                        }
                        _ => {
                            Err(
                                parquetry::error::Error::InvalidField(
                                    "user_name_info".to_string(),
                                ),
                            )
                        }
                    }?;
                    Ok(
                        Some(UserInfo {
                            screen_name,
                            user_name_info,
                        }),
                    )
                }
                _ => Err(parquetry::error::Error::InvalidField("user_info".to_string())),
            }?;
            Ok(User { id, ts, status, user_info })
        }
    }
}
impl User {
    fn write_sort_key_bytes(
        &self,
        column: parquetry::sort::Sort<<Self as parquetry::Schema>::SortColumn>,
        bytes: &mut Vec<u8>,
    ) {
        match column.column {
            columns::SortColumn::Id => {
                let value = self.id;
                for b in value.to_be_bytes() {
                    bytes.push(if column.descending { !b } else { b });
                }
            }
            columns::SortColumn::Ts => {
                let value = self.ts;
                for b in value.timestamp_micros().to_be_bytes() {
                    bytes.push(if column.descending { !b } else { b });
                }
            }
            columns::SortColumn::Status => {
                let value = self.status;
                match value {
                    Some(value) => {
                        bytes.push(if column.nulls_first { 1 } else { 0 });
                        for b in value.to_be_bytes() {
                            bytes.push(if column.descending { !b } else { b });
                        }
                    }
                    None => {
                        bytes.push(if column.nulls_first { 0 } else { 1 });
                    }
                }
            }
            columns::SortColumn::ScreenName => {
                let value = self.user_info.as_ref().map(|value| &value.screen_name);
                match value {
                    Some(value) => {
                        bytes.push(if column.nulls_first { 1 } else { 0 });
                        for b in value.as_bytes() {
                            bytes.push(if column.descending { !b } else { *b });
                        }
                        bytes.push(if column.descending { u8::MAX } else { b'\0' });
                    }
                    None => {
                        bytes.push(if column.nulls_first { 0 } else { 1 });
                    }
                }
            }
            columns::SortColumn::Name => {
                let value = self
                    .user_info
                    .as_ref()
                    .and_then(|value| value.user_name_info.as_ref())
                    .map(|value| &value.name);
                match value {
                    Some(value) => {
                        bytes.push(if column.nulls_first { 1 } else { 0 });
                        for b in value.as_bytes() {
                            bytes.push(if column.descending { !b } else { *b });
                        }
                        bytes.push(if column.descending { u8::MAX } else { b'\0' });
                    }
                    None => {
                        bytes.push(if column.nulls_first { 0 } else { 1 });
                    }
                }
            }
            columns::SortColumn::CreatedAt => {
                let value = self
                    .user_info
                    .as_ref()
                    .and_then(|value| value.user_name_info.as_ref())
                    .and_then(|value| value.user_profile_info.as_ref())
                    .map(|value| value.created_at);
                match value {
                    Some(value) => {
                        bytes.push(if column.nulls_first { 1 } else { 0 });
                        for b in value.timestamp_micros().to_be_bytes() {
                            bytes.push(if column.descending { !b } else { b });
                        }
                    }
                    None => {
                        bytes.push(if column.nulls_first { 0 } else { 1 });
                    }
                }
            }
            columns::SortColumn::CreatedAtDate => {
                let value = self
                    .user_info
                    .as_ref()
                    .and_then(|value| value.user_name_info.as_ref())
                    .and_then(|value| value.user_profile_info.as_ref())
                    .and_then(|value| value.created_at_date.as_ref());
                match value {
                    Some(value) => {
                        bytes.push(if column.nulls_first { 1 } else { 0 });
                        for b in (value
                            .signed_duration_since(
                                chrono::NaiveDate::from_ymd_opt(1970, 1, 1).unwrap(),
                            )
                            .num_days() as i32)
                            .to_be_bytes()
                        {
                            bytes.push(if column.descending { !b } else { b });
                        }
                    }
                    None => {
                        bytes.push(if column.nulls_first { 0 } else { 1 });
                    }
                }
            }
            columns::SortColumn::Location => {
                let value = self
                    .user_info
                    .as_ref()
                    .and_then(|value| value.user_name_info.as_ref())
                    .and_then(|value| value.user_profile_info.as_ref())
                    .map(|value| &value.location);
                match value {
                    Some(value) => {
                        bytes.push(if column.nulls_first { 1 } else { 0 });
                        for b in value.as_bytes() {
                            bytes.push(if column.descending { !b } else { *b });
                        }
                        bytes.push(if column.descending { u8::MAX } else { b'\0' });
                    }
                    None => {
                        bytes.push(if column.nulls_first { 0 } else { 1 });
                    }
                }
            }
            columns::SortColumn::Description => {
                let value = self
                    .user_info
                    .as_ref()
                    .and_then(|value| value.user_name_info.as_ref())
                    .and_then(|value| value.user_profile_info.as_ref())
                    .map(|value| &value.description);
                match value {
                    Some(value) => {
                        bytes.push(if column.nulls_first { 1 } else { 0 });
                        for b in value.as_bytes() {
                            bytes.push(if column.descending { !b } else { *b });
                        }
                        bytes.push(if column.descending { u8::MAX } else { b'\0' });
                    }
                    None => {
                        bytes.push(if column.nulls_first { 0 } else { 1 });
                    }
                }
            }
            columns::SortColumn::Url => {
                let value = self
                    .user_info
                    .as_ref()
                    .and_then(|value| value.user_name_info.as_ref())
                    .and_then(|value| value.user_profile_info.as_ref())
                    .and_then(|value| value.url.as_ref());
                match value {
                    Some(value) => {
                        bytes.push(if column.nulls_first { 1 } else { 0 });
                        for b in value.as_bytes() {
                            bytes.push(if column.descending { !b } else { *b });
                        }
                        bytes.push(if column.descending { u8::MAX } else { b'\0' });
                    }
                    None => {
                        bytes.push(if column.nulls_first { 0 } else { 1 });
                    }
                }
            }
            columns::SortColumn::FollowersCount => {
                let value = self
                    .user_info
                    .as_ref()
                    .and_then(|value| value.user_name_info.as_ref())
                    .and_then(|value| value.user_profile_info.as_ref())
                    .map(|value| value.followers_count);
                match value {
                    Some(value) => {
                        bytes.push(if column.nulls_first { 1 } else { 0 });
                        for b in value.to_be_bytes() {
                            bytes.push(if column.descending { !b } else { b });
                        }
                    }
                    None => {
                        bytes.push(if column.nulls_first { 0 } else { 1 });
                    }
                }
            }
            columns::SortColumn::FriendsCount => {
                let value = self
                    .user_info
                    .as_ref()
                    .and_then(|value| value.user_name_info.as_ref())
                    .and_then(|value| value.user_profile_info.as_ref())
                    .map(|value| value.friends_count);
                match value {
                    Some(value) => {
                        bytes.push(if column.nulls_first { 1 } else { 0 });
                        for b in value.to_be_bytes() {
                            bytes.push(if column.descending { !b } else { b });
                        }
                    }
                    None => {
                        bytes.push(if column.nulls_first { 0 } else { 1 });
                    }
                }
            }
            columns::SortColumn::FavouritesCount => {
                let value = self
                    .user_info
                    .as_ref()
                    .and_then(|value| value.user_name_info.as_ref())
                    .and_then(|value| value.user_profile_info.as_ref())
                    .map(|value| value.favourites_count);
                match value {
                    Some(value) => {
                        bytes.push(if column.nulls_first { 1 } else { 0 });
                        for b in value.to_be_bytes() {
                            bytes.push(if column.descending { !b } else { b });
                        }
                    }
                    None => {
                        bytes.push(if column.nulls_first { 0 } else { 1 });
                    }
                }
            }
            columns::SortColumn::StatusesCount => {
                let value = self
                    .user_info
                    .as_ref()
                    .and_then(|value| value.user_name_info.as_ref())
                    .and_then(|value| value.user_profile_info.as_ref())
                    .map(|value| value.statuses_count);
                match value {
                    Some(value) => {
                        bytes.push(if column.nulls_first { 1 } else { 0 });
                        for b in value.to_be_bytes() {
                            bytes.push(if column.descending { !b } else { b });
                        }
                    }
                    None => {
                        bytes.push(if column.nulls_first { 0 } else { 1 });
                    }
                }
            }
        }
    }
    fn write_with_workspace<W: std::io::Write + Send>(
        file_writer: &mut parquet::file::writer::SerializedFileWriter<W>,
        workspace: &mut ParquetryWorkspace,
    ) -> Result<parquet::file::metadata::RowGroupMetaDataPtr, parquetry::error::Error> {
        {
            let mut row_group_writer = file_writer.next_row_group()?;
            let mut column_writer = row_group_writer
                .next_column()?
                .ok_or_else(|| parquetry::error::Error::InvalidField("id".to_string()))?;
            column_writer
                .typed::<parquet::data_type::Int64Type>()
                .write_batch(&workspace.values_0000, None, None)?;
            column_writer.close()?;
            let mut column_writer = row_group_writer
                .next_column()?
                .ok_or_else(|| parquetry::error::Error::InvalidField("ts".to_string()))?;
            column_writer
                .typed::<parquet::data_type::Int64Type>()
                .write_batch(&workspace.values_0001, None, None)?;
            column_writer.close()?;
            let mut column_writer = row_group_writer
                .next_column()?
                .ok_or_else(|| parquetry::error::Error::InvalidField(
                    "status".to_string(),
                ))?;
            column_writer
                .typed::<parquet::data_type::Int32Type>()
                .write_batch(
                    &workspace.values_0002,
                    Some(&workspace.def_levels_0002),
                    None,
                )?;
            column_writer.close()?;
            let mut column_writer = row_group_writer
                .next_column()?
                .ok_or_else(|| parquetry::error::Error::InvalidField(
                    "screen_name".to_string(),
                ))?;
            column_writer
                .typed::<parquet::data_type::ByteArrayType>()
                .write_batch(
                    &workspace.values_0003,
                    Some(&workspace.def_levels_0003),
                    None,
                )?;
            column_writer.close()?;
            let mut column_writer = row_group_writer
                .next_column()?
                .ok_or_else(|| parquetry::error::Error::InvalidField(
                    "name".to_string(),
                ))?;
            column_writer
                .typed::<parquet::data_type::ByteArrayType>()
                .write_batch(
                    &workspace.values_0004,
                    Some(&workspace.def_levels_0004),
                    None,
                )?;
            column_writer.close()?;
            let mut column_writer = row_group_writer
                .next_column()?
                .ok_or_else(|| parquetry::error::Error::InvalidField(
                    "created_at".to_string(),
                ))?;
            column_writer
                .typed::<parquet::data_type::Int64Type>()
                .write_batch(
                    &workspace.values_0005,
                    Some(&workspace.def_levels_0005),
                    None,
                )?;
            column_writer.close()?;
            let mut column_writer = row_group_writer
                .next_column()?
                .ok_or_else(|| parquetry::error::Error::InvalidField(
                    "created_at_date".to_string(),
                ))?;
            column_writer
                .typed::<parquet::data_type::Int32Type>()
                .write_batch(
                    &workspace.values_0006,
                    Some(&workspace.def_levels_0006),
                    None,
                )?;
            column_writer.close()?;
            let mut column_writer = row_group_writer
                .next_column()?
                .ok_or_else(|| parquetry::error::Error::InvalidField(
                    "location".to_string(),
                ))?;
            column_writer
                .typed::<parquet::data_type::ByteArrayType>()
                .write_batch(
                    &workspace.values_0007,
                    Some(&workspace.def_levels_0007),
                    None,
                )?;
            column_writer.close()?;
            let mut column_writer = row_group_writer
                .next_column()?
                .ok_or_else(|| parquetry::error::Error::InvalidField(
                    "description".to_string(),
                ))?;
            column_writer
                .typed::<parquet::data_type::ByteArrayType>()
                .write_batch(
                    &workspace.values_0008,
                    Some(&workspace.def_levels_0008),
                    None,
                )?;
            column_writer.close()?;
            let mut column_writer = row_group_writer
                .next_column()?
                .ok_or_else(|| parquetry::error::Error::InvalidField(
                    "url".to_string(),
                ))?;
            column_writer
                .typed::<parquet::data_type::ByteArrayType>()
                .write_batch(
                    &workspace.values_0009,
                    Some(&workspace.def_levels_0009),
                    None,
                )?;
            column_writer.close()?;
            let mut column_writer = row_group_writer
                .next_column()?
                .ok_or_else(|| parquetry::error::Error::InvalidField(
                    "followers_count".to_string(),
                ))?;
            column_writer
                .typed::<parquet::data_type::Int32Type>()
                .write_batch(
                    &workspace.values_0010,
                    Some(&workspace.def_levels_0010),
                    None,
                )?;
            column_writer.close()?;
            let mut column_writer = row_group_writer
                .next_column()?
                .ok_or_else(|| parquetry::error::Error::InvalidField(
                    "friends_count".to_string(),
                ))?;
            column_writer
                .typed::<parquet::data_type::Int32Type>()
                .write_batch(
                    &workspace.values_0011,
                    Some(&workspace.def_levels_0011),
                    None,
                )?;
            column_writer.close()?;
            let mut column_writer = row_group_writer
                .next_column()?
                .ok_or_else(|| parquetry::error::Error::InvalidField(
                    "favourites_count".to_string(),
                ))?;
            column_writer
                .typed::<parquet::data_type::Int32Type>()
                .write_batch(
                    &workspace.values_0012,
                    Some(&workspace.def_levels_0012),
                    None,
                )?;
            column_writer.close()?;
            let mut column_writer = row_group_writer
                .next_column()?
                .ok_or_else(|| parquetry::error::Error::InvalidField(
                    "statuses_count".to_string(),
                ))?;
            column_writer
                .typed::<parquet::data_type::Int32Type>()
                .write_batch(
                    &workspace.values_0013,
                    Some(&workspace.def_levels_0013),
                    None,
                )?;
            column_writer.close()?;
            let mut column_writer = row_group_writer
                .next_column()?
                .ok_or_else(|| parquetry::error::Error::InvalidField(
                    "element".to_string(),
                ))?;
            column_writer
                .typed::<parquet::data_type::ByteArrayType>()
                .write_batch(
                    &workspace.values_0014,
                    Some(&workspace.def_levels_0014),
                    Some(&workspace.rep_levels_0014),
                )?;
            column_writer.close()?;
            workspace.clear();
            Ok(row_group_writer.close()?)
        }
    }
    fn fill_workspace<
        'a,
        E: From<parquetry::error::Error>,
        I: Iterator<Item = Result<&'a Self, E>>,
    >(workspace: &mut ParquetryWorkspace, values: I) -> Result<usize, E> {
        {
            let mut written_count = 0;
            for result in values {
                Self::add_item_to_workspace(workspace, result?)?;
                written_count += 1;
            }
            Ok(written_count)
        }
    }
    fn add_item_to_workspace(
        workspace: &mut ParquetryWorkspace,
        value: &Self,
    ) -> Result<(), parquetry::error::Error> {
        {
            let User { id, ts, status, user_info } = value;
            workspace.values_0000.push(*id as i64);
            workspace.values_0001.push(ts.timestamp_millis());
            match status {
                Some(status) => {
                    workspace.values_0002.push(*status);
                    workspace.def_levels_0002.push(1);
                }
                None => {
                    workspace.def_levels_0002.push(0);
                }
            }
            match user_info {
                Some(UserInfo { screen_name, user_name_info }) => {
                    workspace.values_0003.push(screen_name.as_str().into());
                    workspace.def_levels_0003.push(1);
                    match user_name_info {
                        Some(UserNameInfo { name, user_profile_info }) => {
                            workspace.values_0004.push(name.as_str().into());
                            workspace.def_levels_0004.push(2);
                            match user_profile_info {
                                Some(
                                    UserProfileInfo {
                                        created_at,
                                        created_at_date,
                                        location,
                                        description,
                                        url,
                                        followers_count,
                                        friends_count,
                                        favourites_count,
                                        statuses_count,
                                        withheld_in_countries,
                                    },
                                ) => {
                                    workspace.values_0005.push(created_at.timestamp_millis());
                                    workspace.def_levels_0005.push(3);
                                    match created_at_date {
                                        Some(created_at_date) => {
                                            workspace
                                                .values_0006
                                                .push(
                                                    created_at_date
                                                        .signed_duration_since(
                                                            chrono::NaiveDate::from_ymd_opt(1970, 1, 1).unwrap(),
                                                        )
                                                        .num_days() as i32,
                                                );
                                            workspace.def_levels_0006.push(4);
                                        }
                                        None => {
                                            workspace.def_levels_0006.push(3);
                                        }
                                    }
                                    workspace.values_0007.push(location.as_str().into());
                                    workspace.def_levels_0007.push(3);
                                    workspace.values_0008.push(description.as_str().into());
                                    workspace.def_levels_0008.push(3);
                                    match url {
                                        Some(url) => {
                                            workspace.values_0009.push(url.as_str().into());
                                            workspace.def_levels_0009.push(4);
                                        }
                                        None => {
                                            workspace.def_levels_0009.push(3);
                                        }
                                    }
                                    workspace.values_0010.push(*followers_count);
                                    workspace.def_levels_0010.push(3);
                                    workspace.values_0011.push(*friends_count);
                                    workspace.def_levels_0011.push(3);
                                    workspace.values_0012.push(*favourites_count);
                                    workspace.def_levels_0012.push(3);
                                    workspace.values_0013.push(*statuses_count);
                                    workspace.def_levels_0013.push(3);
                                    match withheld_in_countries {
                                        Some(withheld_in_countries) => {
                                            if withheld_in_countries.is_empty() {
                                                workspace.def_levels_0014.push(4);
                                                workspace.rep_levels_0014.push(0);
                                            } else {
                                                let mut first = true;
                                                for element in withheld_in_countries {
                                                    if first {
                                                        workspace.values_0014.push(element.as_str().into());
                                                        workspace.def_levels_0014.push(5);
                                                        workspace.rep_levels_0014.push(0);
                                                        first = false;
                                                    } else {
                                                        workspace.values_0014.push(element.as_str().into());
                                                        workspace.def_levels_0014.push(5);
                                                        workspace.rep_levels_0014.push(1);
                                                    }
                                                }
                                            }
                                        }
                                        None => {
                                            workspace.def_levels_0014.push(3);
                                            workspace.rep_levels_0014.push(0);
                                        }
                                    }
                                }
                                None => {
                                    workspace.def_levels_0005.push(2);
                                    workspace.def_levels_0006.push(2);
                                    workspace.def_levels_0007.push(2);
                                    workspace.def_levels_0008.push(2);
                                    workspace.def_levels_0009.push(2);
                                    workspace.def_levels_0010.push(2);
                                    workspace.def_levels_0011.push(2);
                                    workspace.def_levels_0012.push(2);
                                    workspace.def_levels_0013.push(2);
                                    workspace.def_levels_0014.push(2);
                                    workspace.rep_levels_0014.push(0);
                                }
                            }
                        }
                        None => {
                            workspace.def_levels_0004.push(1);
                            workspace.def_levels_0005.push(1);
                            workspace.def_levels_0006.push(1);
                            workspace.def_levels_0007.push(1);
                            workspace.def_levels_0008.push(1);
                            workspace.def_levels_0009.push(1);
                            workspace.def_levels_0010.push(1);
                            workspace.def_levels_0011.push(1);
                            workspace.def_levels_0012.push(1);
                            workspace.def_levels_0013.push(1);
                            workspace.def_levels_0014.push(1);
                            workspace.rep_levels_0014.push(0);
                        }
                    }
                }
                None => {
                    workspace.def_levels_0003.push(0);
                    workspace.def_levels_0004.push(0);
                    workspace.def_levels_0005.push(0);
                    workspace.def_levels_0006.push(0);
                    workspace.def_levels_0007.push(0);
                    workspace.def_levels_0008.push(0);
                    workspace.def_levels_0009.push(0);
                    workspace.def_levels_0010.push(0);
                    workspace.def_levels_0011.push(0);
                    workspace.def_levels_0012.push(0);
                    workspace.def_levels_0013.push(0);
                    workspace.def_levels_0014.push(0);
                    workspace.rep_levels_0014.push(0);
                }
            }
            Ok(())
        }
    }
}
impl User {
    pub fn new(
        id: u64,
        ts: chrono::DateTime<chrono::Utc>,
        status: Option<i32>,
        user_info: Option<UserInfo>,
    ) -> Result<Self, parquetry::error::ValueError> {
        let ts = chrono::SubsecRound::trunc_subsecs(ts, 3);
        Ok(Self { id, ts, status, user_info })
    }
}
impl UserInfo {
    pub fn new(
        screen_name: String,
        user_name_info: Option<UserNameInfo>,
    ) -> Result<Self, parquetry::error::ValueError> {
        for (index, byte) in screen_name.as_bytes().iter().enumerate() {
            if *byte == 0 {
                return Err(parquetry::error::ValueError::NullByteString {
                    column_path: parquet::schema::types::ColumnPath::new(
                        vec!["user_info".to_string(), "screen_name".to_string(),],
                    ),
                    index,
                });
            }
        }
        Ok(Self {
            screen_name,
            user_name_info,
        })
    }
}
impl UserNameInfo {
    pub fn new(
        name: String,
        user_profile_info: Option<UserProfileInfo>,
    ) -> Result<Self, parquetry::error::ValueError> {
        for (index, byte) in name.as_bytes().iter().enumerate() {
            if *byte == 0 {
                return Err(parquetry::error::ValueError::NullByteString {
                    column_path: parquet::schema::types::ColumnPath::new(
                        vec![
                            "user_info".to_string(), "user_name_info".to_string(), "name"
                            .to_string(),
                        ],
                    ),
                    index,
                });
            }
        }
        Ok(Self { name, user_profile_info })
    }
}
impl UserProfileInfo {
    pub fn new(
        created_at: chrono::DateTime<chrono::Utc>,
        created_at_date: Option<chrono::NaiveDate>,
        location: String,
        description: String,
        url: Option<String>,
        followers_count: i32,
        friends_count: i32,
        favourites_count: i32,
        statuses_count: i32,
        withheld_in_countries: Option<Vec<String>>,
    ) -> Result<Self, parquetry::error::ValueError> {
        let created_at = chrono::SubsecRound::trunc_subsecs(created_at, 3);
        for (index, byte) in location.as_bytes().iter().enumerate() {
            if *byte == 0 {
                return Err(parquetry::error::ValueError::NullByteString {
                    column_path: parquet::schema::types::ColumnPath::new(
                        vec![
                            "user_info".to_string(), "user_name_info".to_string(),
                            "user_profile_info".to_string(), "location".to_string(),
                        ],
                    ),
                    index,
                });
            }
        }
        for (index, byte) in description.as_bytes().iter().enumerate() {
            if *byte == 0 {
                return Err(parquetry::error::ValueError::NullByteString {
                    column_path: parquet::schema::types::ColumnPath::new(
                        vec![
                            "user_info".to_string(), "user_name_info".to_string(),
                            "user_profile_info".to_string(), "description".to_string(),
                        ],
                    ),
                    index,
                });
            }
        }
        if let Some(url) = url.as_ref() {
            for (index, byte) in url.as_bytes().iter().enumerate() {
                if *byte == 0 {
                    return Err(parquetry::error::ValueError::NullByteString {
                        column_path: parquet::schema::types::ColumnPath::new(
                            vec![
                                "user_info".to_string(), "user_name_info".to_string(),
                                "user_profile_info".to_string(), "url".to_string(),
                            ],
                        ),
                        index,
                    });
                }
            }
        }
        Ok(Self {
            created_at,
            created_at_date,
            location,
            description,
            url,
            followers_count,
            friends_count,
            favourites_count,
            statuses_count,
            withheld_in_countries,
        })
    }
}
#[derive(Default)]
struct ParquetryWorkspace {
    values_0000: Vec<i64>,
    values_0001: Vec<i64>,
    values_0002: Vec<i32>,
    def_levels_0002: Vec<i16>,
    values_0003: Vec<parquet::data_type::ByteArray>,
    def_levels_0003: Vec<i16>,
    values_0004: Vec<parquet::data_type::ByteArray>,
    def_levels_0004: Vec<i16>,
    values_0005: Vec<i64>,
    def_levels_0005: Vec<i16>,
    values_0006: Vec<i32>,
    def_levels_0006: Vec<i16>,
    values_0007: Vec<parquet::data_type::ByteArray>,
    def_levels_0007: Vec<i16>,
    values_0008: Vec<parquet::data_type::ByteArray>,
    def_levels_0008: Vec<i16>,
    values_0009: Vec<parquet::data_type::ByteArray>,
    def_levels_0009: Vec<i16>,
    values_0010: Vec<i32>,
    def_levels_0010: Vec<i16>,
    values_0011: Vec<i32>,
    def_levels_0011: Vec<i16>,
    values_0012: Vec<i32>,
    def_levels_0012: Vec<i16>,
    values_0013: Vec<i32>,
    def_levels_0013: Vec<i16>,
    values_0014: Vec<parquet::data_type::ByteArray>,
    def_levels_0014: Vec<i16>,
    rep_levels_0014: Vec<i16>,
}
impl ParquetryWorkspace {
    fn clear(&mut self) {
        self.values_0000.clear();
        self.values_0001.clear();
        self.values_0002.clear();
        self.def_levels_0002.clear();
        self.values_0003.clear();
        self.def_levels_0003.clear();
        self.values_0004.clear();
        self.def_levels_0004.clear();
        self.values_0005.clear();
        self.def_levels_0005.clear();
        self.values_0006.clear();
        self.def_levels_0006.clear();
        self.values_0007.clear();
        self.def_levels_0007.clear();
        self.values_0008.clear();
        self.def_levels_0008.clear();
        self.values_0009.clear();
        self.def_levels_0009.clear();
        self.values_0010.clear();
        self.def_levels_0010.clear();
        self.values_0011.clear();
        self.def_levels_0011.clear();
        self.values_0012.clear();
        self.def_levels_0012.clear();
        self.values_0013.clear();
        self.def_levels_0013.clear();
        self.values_0014.clear();
        self.def_levels_0014.clear();
        self.rep_levels_0014.clear();
    }
}
#[cfg(test)]
mod test {
    impl quickcheck::Arbitrary for super::User {
        fn arbitrary(g: &mut quickcheck::Gen) -> Self {
            Self::new(
                    <_>::arbitrary(g),
                    chrono::SubsecRound::trunc_subsecs(
                        chrono::TimeZone::timestamp_millis_opt(
                                &chrono::Utc,
                                gen_valid_timestamp_milli(g),
                            )
                            .single()
                            .expect(
                                "Invalid quickcheck::Arbitrary instance for DateTime<Utc>",
                            ),
                        3,
                    ),
                    <_>::arbitrary(g),
                    <_>::arbitrary(g),
                )
                .expect("Invalid quickcheck::Arbitrary instance for User")
        }
    }
    impl quickcheck::Arbitrary for super::UserInfo {
        fn arbitrary(g: &mut quickcheck::Gen) -> Self {
            Self::new(
                    {
                        let mut value: String = quickcheck::Arbitrary::arbitrary(g);
                        value.retain(|char| char != '\0');
                        value
                    },
                    <_>::arbitrary(g),
                )
                .expect("Invalid quickcheck::Arbitrary instance for UserInfo")
        }
    }
    impl quickcheck::Arbitrary for super::UserNameInfo {
        fn arbitrary(g: &mut quickcheck::Gen) -> Self {
            Self::new(
                    {
                        let mut value: String = quickcheck::Arbitrary::arbitrary(g);
                        value.retain(|char| char != '\0');
                        value
                    },
                    <_>::arbitrary(g),
                )
                .expect("Invalid quickcheck::Arbitrary instance for UserNameInfo")
        }
    }
    impl quickcheck::Arbitrary for super::UserProfileInfo {
        fn arbitrary(g: &mut quickcheck::Gen) -> Self {
            Self::new(
                    chrono::SubsecRound::trunc_subsecs(
                        chrono::TimeZone::timestamp_millis_opt(
                                &chrono::Utc,
                                gen_valid_timestamp_milli(g),
                            )
                            .single()
                            .expect(
                                "Invalid quickcheck::Arbitrary instance for DateTime<Utc>",
                            ),
                        3,
                    ),
                    {
                        let optional: Option<()> = <_>::arbitrary(g);
                        optional
                            .map(|_| {
                                chrono::TimeDelta::try_days(gen_valid_date(g))
                                    .and_then(|delta| {
                                        chrono::NaiveDate::from_ymd_opt(1970, 1, 1)
                                            .unwrap()
                                            .checked_add_signed(delta)
                                    })
                                    .expect(
                                        "Invalid quickcheck::Arbitrary instance for NaiveDate",
                                    )
                            })
                    },
                    {
                        let mut value: String = quickcheck::Arbitrary::arbitrary(g);
                        value.retain(|char| char != '\0');
                        value
                    },
                    {
                        let mut value: String = quickcheck::Arbitrary::arbitrary(g);
                        value.retain(|char| char != '\0');
                        value
                    },
                    {
                        let optional: Option<()> = <_>::arbitrary(g);
                        optional
                            .map(|_| {
                                let mut value: String = quickcheck::Arbitrary::arbitrary(g);
                                value.retain(|char| char != '\0');
                                value
                            })
                    },
                    <_>::arbitrary(g),
                    <_>::arbitrary(g),
                    <_>::arbitrary(g),
                    <_>::arbitrary(g),
                    <_>::arbitrary(g),
                )
                .expect("Invalid quickcheck::Arbitrary instance for UserProfileInfo")
        }
    }
    fn round_trip_write_impl(groups: Vec<Vec<super::User>>) -> bool {
        let test_dir = tempfile::Builder::new().prefix("User-data").tempdir().unwrap();
        let test_file_path = test_dir.path().join("write-data.parquet");
        let test_file = std::fs::File::create(&test_file_path).unwrap();
        <super::User as parquetry::Schema>::write_row_groups(
                test_file,
                Default::default(),
                groups.clone(),
            )
            .unwrap();
        let read_file = std::fs::File::open(test_file_path).unwrap();
        let read_options = parquet::file::serialized_reader::ReadOptionsBuilder::new()
            .build();
        let read_values = <super::User as parquetry::Schema>::read(
                read_file,
                read_options,
            )
            .collect::<Result<Vec<_>, _>>()
            .unwrap();
        read_values == groups.into_iter().flatten().collect::<Vec<_>>()
    }
    quickcheck::quickcheck! {
        fn round_trip_write(groups : Vec < Vec < super::User >>) -> bool {
        round_trip_write_impl(groups) }
    }
    fn round_trip_serde_bincode_impl(values: Vec<super::User>) -> bool {
        let wrapped = bincode::serde::Compat(&values);
        let encoded = bincode::encode_to_vec(&wrapped, bincode::config::standard())
            .unwrap();
        let decoded: (bincode::serde::Compat<Vec<super::User>>, _) = bincode::decode_from_slice(
                &encoded.as_slice(),
                bincode::config::standard(),
            )
            .unwrap();
        decoded.0.0 == values
    }
    quickcheck::quickcheck! {
        fn round_trip_serde_bincode(values : Vec < super::User >) -> bool {
        round_trip_serde_bincode_impl(values) }
    }
    fn gen_valid_date(g: &mut quickcheck::Gen) -> i64 {
        {
            use quickcheck::Arbitrary;
            let value: u16 = <_>::arbitrary(g);
            value as i64
        }
    }
    fn gen_valid_timestamp_milli(g: &mut quickcheck::Gen) -> i64 {
        {
            use quickcheck::Arbitrary;
            let min = chrono::DateTime::<chrono::Utc>::MIN_UTC.timestamp_millis();
            let max = chrono::DateTime::<chrono::Utc>::MAX_UTC.timestamp_millis();
            let value: i64 = <_>::arbitrary(g);
            if value < min {
                value % min
            } else if value > max {
                value % max
            } else {
                value
            }
        }
    }
    fn gen_valid_timestamp_micro(g: &mut quickcheck::Gen) -> i64 {
        {
            use quickcheck::Arbitrary;
            let min = chrono::DateTime::<chrono::Utc>::MIN_UTC.timestamp_micros();
            let max = chrono::DateTime::<chrono::Utc>::MAX_UTC.timestamp_micros();
            let value: i64 = <_>::arbitrary(g);
            if value < min {
                value % min
            } else if value > max {
                value % max
            } else {
                value
            }
        }
    }
}
