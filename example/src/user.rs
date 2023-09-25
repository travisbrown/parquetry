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
lazy_static::lazy_static! {
    pub static ref SCHEMA : parquet::schema::types::SchemaDescPtr =
    std::sync::Arc::new(parquet::schema::types::SchemaDescriptor::new(std::sync::Arc::new(parquet::schema::parser::parse_message_type(SCHEMA_SOURCE)
    .unwrap())));
}
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
        Location,
        Description,
        Url,
        FollowersCount,
        FriendsCount,
        FavouritesCount,
        StatusesCount,
    }
    impl parquetry::SortColumn for SortColumn {
        fn index(&self) -> usize {
            match self {
                Self::Id => 0,
                Self::Ts => 1,
                Self::Status => 2,
                Self::ScreenName => 3,
                Self::Name => 4,
                Self::CreatedAt => 5,
                Self::Location => 6,
                Self::Description => 7,
                Self::Url => 8,
                Self::FollowersCount => 9,
                Self::FriendsCount => 10,
                Self::FavouritesCount => 11,
                Self::StatusesCount => 12,
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
                pub const LOCATION: parquetry::ColumnInfo = parquetry::ColumnInfo {
                    index: 6,
                    path: &[
                        "user_info",
                        "user_name_info",
                        "user_profile_info",
                        "location",
                    ],
                };
                pub const DESCRIPTION: parquetry::ColumnInfo = parquetry::ColumnInfo {
                    index: 7,
                    path: &[
                        "user_info",
                        "user_name_info",
                        "user_profile_info",
                        "description",
                    ],
                };
                pub const URL: parquetry::ColumnInfo = parquetry::ColumnInfo {
                    index: 8,
                    path: &["user_info", "user_name_info", "user_profile_info", "url"],
                };
                pub const FOLLOWERS_COUNT: parquetry::ColumnInfo = parquetry::ColumnInfo {
                    index: 9,
                    path: &[
                        "user_info",
                        "user_name_info",
                        "user_profile_info",
                        "followers_count",
                    ],
                };
                pub const FRIENDS_COUNT: parquetry::ColumnInfo = parquetry::ColumnInfo {
                    index: 10,
                    path: &[
                        "user_info",
                        "user_name_info",
                        "user_profile_info",
                        "friends_count",
                    ],
                };
                pub const FAVOURITES_COUNT: parquetry::ColumnInfo = parquetry::ColumnInfo {
                    index: 11,
                    path: &[
                        "user_info",
                        "user_name_info",
                        "user_profile_info",
                        "favourites_count",
                    ],
                };
                pub const STATUSES_COUNT: parquetry::ColumnInfo = parquetry::ColumnInfo {
                    index: 12,
                    path: &[
                        "user_info",
                        "user_name_info",
                        "user_profile_info",
                        "statuses_count",
                    ],
                };
                pub const WITHHELD_IN_COUNTRIES: parquetry::ColumnInfo = parquetry::ColumnInfo {
                    index: 13,
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
    fn sort_key_value(&self, sort_key: parquetry::SortKey<Self::SortColumn>) -> Vec<u8> {
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
    fn write<W: std::io::Write + Send, I: IntoIterator<Item = Vec<Self>>>(
        writer: W,
        properties: parquet::file::properties::WriterProperties,
        groups: I,
    ) -> Result<parquet::format::FileMetaData, parquetry::error::Error> {
        {
            let mut file_writer = parquet::file::writer::SerializedFileWriter::new(
                writer,
                SCHEMA.root_schema_ptr(),
                std::sync::Arc::new(properties),
            )?;
            let mut workspace = ParquetryWorkspace::default();
            for group in groups {
                Self::fill_workspace(&mut workspace, &group)?;
                Self::write_with_workspace(&mut file_writer, &mut workspace)?;
            }
            Ok(file_writer.close()?)
        }
    }
    fn write_group<W: std::io::Write + Send>(
        file_writer: &mut parquet::file::writer::SerializedFileWriter<W>,
        group: &[Self],
    ) -> Result<parquet::file::metadata::RowGroupMetaDataPtr, parquetry::error::Error> {
        {
            let mut workspace = ParquetryWorkspace::default();
            Self::fill_workspace(&mut workspace, group)?;
            Self::write_with_workspace(file_writer, &mut workspace)
        }
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
                                "value".to_string(),
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
                                                        "value".to_string(),
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
                                                                "Vec<String>".to_string(),
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
        column: parquetry::Sort<<Self as parquetry::Schema>::SortColumn>,
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
                    "location".to_string(),
                ))?;
            column_writer
                .typed::<parquet::data_type::ByteArrayType>()
                .write_batch(
                    &workspace.values_0006,
                    Some(&workspace.def_levels_0006),
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
                    &workspace.values_0007,
                    Some(&workspace.def_levels_0007),
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
                    &workspace.values_0008,
                    Some(&workspace.def_levels_0008),
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
                    &workspace.values_0009,
                    Some(&workspace.def_levels_0009),
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
                    &workspace.values_0010,
                    Some(&workspace.def_levels_0010),
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
                    &workspace.values_0011,
                    Some(&workspace.def_levels_0011),
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
                    &workspace.values_0012,
                    Some(&workspace.def_levels_0012),
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
                    &workspace.values_0013,
                    Some(&workspace.def_levels_0013),
                    Some(&workspace.rep_levels_0013),
                )?;
            column_writer.close()?;
            workspace.clear();
            Ok(row_group_writer.close()?)
        }
    }
    fn fill_workspace(
        workspace: &mut ParquetryWorkspace,
        group: &[Self],
    ) -> Result<usize, parquetry::error::Error> {
        {
            let mut written_count_ = 0;
            for User { id, ts, status, user_info } in group {
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
                                        workspace.values_0006.push(location.as_str().into());
                                        workspace.def_levels_0006.push(3);
                                        workspace.values_0007.push(description.as_str().into());
                                        workspace.def_levels_0007.push(3);
                                        match url {
                                            Some(url) => {
                                                workspace.values_0008.push(url.as_str().into());
                                                workspace.def_levels_0008.push(4);
                                            }
                                            None => {
                                                workspace.def_levels_0008.push(3);
                                            }
                                        }
                                        workspace.values_0009.push(*followers_count);
                                        workspace.def_levels_0009.push(3);
                                        workspace.values_0010.push(*friends_count);
                                        workspace.def_levels_0010.push(3);
                                        workspace.values_0011.push(*favourites_count);
                                        workspace.def_levels_0011.push(3);
                                        workspace.values_0012.push(*statuses_count);
                                        workspace.def_levels_0012.push(3);
                                        match withheld_in_countries {
                                            Some(withheld_in_countries) => {
                                                if withheld_in_countries.is_empty() {
                                                    workspace.def_levels_0013.push(4);
                                                    workspace.rep_levels_0013.push(0);
                                                } else {
                                                    let mut first = true;
                                                    for element in withheld_in_countries {
                                                        if first {
                                                            workspace.values_0013.push(element.as_str().into());
                                                            workspace.def_levels_0013.push(5);
                                                            workspace.rep_levels_0013.push(0);
                                                            first = false;
                                                        } else {
                                                            workspace.values_0013.push(element.as_str().into());
                                                            workspace.def_levels_0013.push(5);
                                                            workspace.rep_levels_0013.push(1);
                                                        }
                                                    }
                                                }
                                            }
                                            None => {
                                                workspace.def_levels_0013.push(3);
                                                workspace.rep_levels_0013.push(0);
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
                                        workspace.rep_levels_0013.push(0);
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
                                workspace.rep_levels_0013.push(0);
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
                        workspace.rep_levels_0013.push(0);
                    }
                }
                written_count_ += 1;
            }
            Ok(written_count_)
        }
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
    values_0006: Vec<parquet::data_type::ByteArray>,
    def_levels_0006: Vec<i16>,
    values_0007: Vec<parquet::data_type::ByteArray>,
    def_levels_0007: Vec<i16>,
    values_0008: Vec<parquet::data_type::ByteArray>,
    def_levels_0008: Vec<i16>,
    values_0009: Vec<i32>,
    def_levels_0009: Vec<i16>,
    values_0010: Vec<i32>,
    def_levels_0010: Vec<i16>,
    values_0011: Vec<i32>,
    def_levels_0011: Vec<i16>,
    values_0012: Vec<i32>,
    def_levels_0012: Vec<i16>,
    values_0013: Vec<parquet::data_type::ByteArray>,
    def_levels_0013: Vec<i16>,
    rep_levels_0013: Vec<i16>,
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
        self.rep_levels_0013.clear();
    }
}
#[cfg(test)]
mod test {
    impl quickcheck::Arbitrary for super::User {
        fn arbitrary(g: &mut quickcheck::Gen) -> Self {
            Self {
                id: <_>::arbitrary(g),
                ts: chrono::SubsecRound::trunc_subsecs(
                    chrono::TimeZone::timestamp_millis_opt(
                            &chrono::Utc,
                            gen_valid_timestamp_milli(g),
                        )
                        .single()
                        .expect("Arbitrary instance for DateTime<Utc> is invalid"),
                    3,
                ),
                status: <_>::arbitrary(g),
                user_info: <_>::arbitrary(g),
            }
        }
    }
    impl quickcheck::Arbitrary for super::UserInfo {
        fn arbitrary(g: &mut quickcheck::Gen) -> Self {
            Self {
                screen_name: <_>::arbitrary(g),
                user_name_info: <_>::arbitrary(g),
            }
        }
    }
    impl quickcheck::Arbitrary for super::UserNameInfo {
        fn arbitrary(g: &mut quickcheck::Gen) -> Self {
            Self {
                name: <_>::arbitrary(g),
                user_profile_info: <_>::arbitrary(g),
            }
        }
    }
    impl quickcheck::Arbitrary for super::UserProfileInfo {
        fn arbitrary(g: &mut quickcheck::Gen) -> Self {
            Self {
                created_at: chrono::SubsecRound::trunc_subsecs(
                    chrono::TimeZone::timestamp_millis_opt(
                            &chrono::Utc,
                            gen_valid_timestamp_milli(g),
                        )
                        .single()
                        .expect("Arbitrary instance for DateTime<Utc> is invalid"),
                    3,
                ),
                location: <_>::arbitrary(g),
                description: <_>::arbitrary(g),
                url: <_>::arbitrary(g),
                followers_count: <_>::arbitrary(g),
                friends_count: <_>::arbitrary(g),
                favourites_count: <_>::arbitrary(g),
                statuses_count: <_>::arbitrary(g),
                withheld_in_countries: <_>::arbitrary(g),
            }
        }
    }
    fn round_trip_write_impl(groups: Vec<Vec<super::User>>) -> bool {
        let test_dir = tempdir::TempDir::new("User-data").unwrap();
        let test_file_path = test_dir.path().join("write-data.parquet");
        let test_file = std::fs::File::create(&test_file_path).unwrap();
        <super::User as parquetry::Schema>::write(
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
    fn round_trip_write_group_impl(groups: Vec<Vec<super::User>>) -> bool {
        let test_dir = tempdir::TempDir::new("User-data").unwrap();
        let test_file_path = test_dir.path().join("write_group-data.parquet");
        let test_file = std::fs::File::create(&test_file_path).unwrap();
        let mut file_writer = parquet::file::writer::SerializedFileWriter::new(
                test_file,
                <super::User as parquetry::Schema>::schema().root_schema_ptr(),
                Default::default(),
            )
            .unwrap();
        for group in &groups {
            <super::User as parquetry::Schema>::write_group(&mut file_writer, group)
                .unwrap();
        }
        file_writer.close().unwrap();
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
        fn round_trip_write_group(groups : Vec < Vec < super::User >>) -> bool {
        round_trip_write_group_impl(groups) }
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
