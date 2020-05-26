{-# LANGUAGE GeneralizedNewtypeDeriving #-}
{-# LANGUAGE CPP #-}
module Database.Beam.MySQL.Connection
    ( MySQL(..), MySQL.Connection
    , MySQLM(..)

    , runBeamMySQL, runBeamMySQLDebug

    , MysqlCommandSyntax(..)
    , MysqlSelectSyntax(..), MysqlInsertSyntax(..)
    , MysqlUpdateSyntax(..), MysqlDeleteSyntax(..)
    , MysqlExpressionSyntax(..)

    , runInsertRowReturning

    , MySQL.connect, MySQL.close

    , mysqlUriSyntax ) where

import           Database.Beam.MySQL.Syntax
import           Database.Beam.MySQL.FromField
import           Database.Beam.Backend
import           Database.Beam.Backend.URI
import qualified Database.Beam.Backend.SQL.BeamExtensions as Beam
import           Database.Beam.Query
import           Database.Beam.Query.SQL92

import           Database.MySQL.Base as MySQL
import qualified Database.MySQL.Base.Types as MySQL

import           Control.Exception
import           Control.Monad.Except
import           Control.Monad.Fail (MonadFail)
import qualified Control.Monad.Fail as Fail
import           Control.Monad.Free.Church
import           Control.Monad.Reader

import qualified Data.Aeson as A (Value)
import           Data.ByteString.Builder
import qualified Data.ByteString.Char8 as BS
import qualified Data.ByteString.Lazy as BL
import           Data.Int
import           Data.List
import           Data.Maybe
import           Data.Ratio
import           Data.Scientific
import qualified Data.Text as T
import qualified Data.Text.Encoding as TE
import qualified Data.Text.Encoding.Error as TE
import qualified Data.Text.Lazy as TL
import           Data.Time (Day, LocalTime, NominalDiffTime, TimeOfDay)
import           Data.Word
import           Data.Functor.Identity
import           Network.URI
import           Text.Read hiding (step)

data MySQL = MySQL

instance BeamSqlBackendIsString MySQL String
instance BeamSqlBackendIsString MySQL T.Text

instance BeamBackend MySQL where
    type BackendFromField MySQL = FromField

instance BeamSqlBackend MySQL
type instance BeamSqlBackendSyntax MySQL = MysqlCommandSyntax

newtype MySQLM a = MySQLM (ReaderT (String -> IO (), Connection) IO a)
    deriving (Monad, MonadIO, Applicative, Functor)

instance MonadFail MySQLM where
    fail e = fail $ "Internal Error with: " <> show e

data NotEnoughColumns
    = NotEnoughColumns
    { _errColCount :: Int
    } deriving Show



instance Exception NotEnoughColumns where
    displayException (NotEnoughColumns colCnt) =
        mconcat [ "Not enough columns while reading MySQL row. Only have "
                , show colCnt, " column(s)" ]

data CouldNotReadColumn
  = CouldNotReadColumn
  { _errColIndex :: Int
  , _errColMsg   :: String }
  deriving Show

instance Exception CouldNotReadColumn where
  displayException (CouldNotReadColumn idx msg) =
    mconcat [ "Could not read column ", show idx, ": ", msg ]

runBeamMySQLDebug :: (String -> IO ()) -> Connection -> MySQLM a -> IO a
runBeamMySQLDebug = withMySQL

runBeamMySQL :: Connection -> MySQLM a -> IO a
runBeamMySQL = runBeamMySQLDebug (\_ -> pure ())

instance MonadBeam MySQL MySQLM where
    runReturningMany (MysqlCommandSyntax (MysqlSyntax cmd))
                     (consume :: MySQLM (Maybe x) -> MySQLM a) =
        MySQLM . ReaderT $ \(dbg, conn) -> do
          cmdBuilder <- cmd (\_ b _ -> pure b) (MySQL.escape conn) mempty conn
          let cmdStr = BL.toStrict (toLazyByteString cmdBuilder)

          dbg (T.unpack (TE.decodeUtf8With TE.lenientDecode cmdStr))

          MySQL.query conn cmdStr

          bracket (useResult conn) freeResult $ \res -> do
            fieldDescs <- MySQL.fetchFields res

            let fetchRow' :: MySQLM (Maybe x)
                fetchRow' =
                  MySQLM . ReaderT $ \_ -> do
                    fields <- MySQL.fetchRow res

                    case fields of
                      [] -> pure Nothing
                      _ -> do
                        let FromBackendRowM go = fromBackendRow
                        rowRes <- runF go (\x _ _ -> pure (Right x)) step
                                    0 (zip fieldDescs fields)
                        case rowRes of
                          Left err -> throwIO err
                          Right x -> pure (Just x)

                parseField :: forall field. FromField field
                           => MySQL.Field -> Maybe BS.ByteString
                           -> IO (Either ColumnParseError field)
                parseField ty d = runExceptT (fromField ty d)

                step :: forall y
                      . FromBackendRowF MySQL (Int -> [(MySQL.Field, Maybe BS.ByteString)] -> IO (Either BeamRowReadError y))
                     -> Int -> [(MySQL.Field, Maybe BS.ByteString)] -> IO (Either BeamRowReadError y)
                step (ParseOneField _) curCol [] =
                    pure (Left (BeamRowReadError (Just curCol) (ColumnNotEnoughColumns curCol)))

                step (ParseOneField next) curCol ((desc, field):fields) =
                    do d <- parseField desc field
                       case d of
                         Left  e  -> pure (Left (BeamRowReadError (Just curCol) e))
                         Right d' -> next d' (curCol + 1) fields

                step (Alt (FromBackendRowM a) (FromBackendRowM b) next) curCol cols =
                    do aRes <- runF a (\x curCol' cols' -> pure (Right (next x curCol' cols'))) step curCol cols
                       case aRes of
                         Right next' -> next'
                         Left aErr -> do
                           bRes <- runF b (\x curCol' cols' -> pure (Right (next x curCol' cols'))) step curCol cols
                           case bRes of
                             Right next' -> next'
                             Left _ -> pure (Left aErr)

                step (FailParseWith err) _ _ = pure (Left err)

                MySQLM doConsume = consume fetchRow'

            runReaderT doConsume (dbg, conn)

withMySQL :: (String -> IO ()) -> Connection
          -> MySQLM a -> IO a
withMySQL dbg conn (MySQLM a) =
    runReaderT a (dbg, conn)

mysqlUriSyntax :: c MySQL Connection MySQLM
               -> BeamURIOpeners c
mysqlUriSyntax =
    mkUriOpener (withMySQL (const (pure ()))) "mysql:"
        (\uri ->
             let stripSuffix s a =
                     reverse <$> stripPrefix (reverse s) (reverse a)

                 (user, pw) =
                     fromMaybe ("root", "") $ do
                       userInfo  <- fmap uriUserInfo (uriAuthority uri)
                       userInfo' <- stripSuffix "@" userInfo
                       let (user', pw') = break (== ':') userInfo'
                           pw'' = fromMaybe "" (stripPrefix ":" pw')
                       pure (user', pw'')
                 host =
                     fromMaybe "localhost" .
                     fmap uriRegName . uriAuthority $ uri
                 port =
                     fromMaybe 3306 $ do
                       portStr <- fmap uriPort (uriAuthority uri)
                       portStr' <- stripPrefix ":" portStr
                       readMaybe portStr'

                 db = fromMaybe "test" $
                      stripPrefix "/" (uriPath uri)

                 options =
                   fromMaybe [CharsetName "utf-8"] $ do
                     opts <- stripPrefix "?" (uriQuery uri)
                     let getKeyValuePairs "" a = a []
                         getKeyValuePairs d a =
                             let (keyValue, d') = break (=='&') d
                                 attr = parseKeyValue keyValue
                             in getKeyValuePairs d' (a . maybe id (:) attr)

                     pure (getKeyValuePairs opts id)

                 parseBool (Just "true") = pure True
                 parseBool (Just "false") = pure False
                 parseBool _ = Nothing

                 parseKeyValue kv = do
                   let (key, value) = break (==':') kv
                       value' = stripPrefix ":" value

                   case (key, value') of
                     ("connectTimeout", Just secs) ->
                         ConnectTimeout <$> readMaybe secs
                     ( "compress", _) -> pure Compress
                     ( "namedPipe", _ ) -> pure NamedPipe
                     ( "initCommand", Just cmd ) ->
                         pure (InitCommand (BS.pack cmd))
                     ( "readDefaultFile", Just fp ) ->
                         pure (ReadDefaultFile fp)
                     ( "readDefaultGroup", Just grp ) ->
                         pure (ReadDefaultGroup (BS.pack grp))
                     ( "charsetDir", Just fp ) ->
                         pure (CharsetDir fp)
                     ( "charsetName", Just nm ) ->
                         pure (CharsetName nm)
                     ( "localInFile", b ) ->
                         LocalInFile <$> parseBool b
                     ( "protocol", Just p) ->
                         case p of
                           "tcp" -> pure (Protocol TCP)
                           "socket" -> pure (Protocol Socket)
                           "pipe" -> pure (Protocol Pipe)
                           "memory" -> pure (Protocol Memory)
                           _ -> Nothing
                     ( "sharedMemoryBaseName", Just fp ) ->
                         pure (SharedMemoryBaseName (BS.pack fp))
                     ( "readTimeout", Just secs ) ->
                         ReadTimeout <$> readMaybe secs
                     ( "writeTimeout", Just secs ) ->
                        WriteTimeout <$> readMaybe secs
                     -- ( "useRemoteConnection", _ ) -> pure UseRemoteConnection
                     -- ( "useEmbeddedConnection", _ ) -> pure UseEmbeddedConnection
                     -- ( "guessConnection", _ ) -> pure GuessConnection
                     -- ( "clientIp", Just fp) -> pure (ClientIP (BS.pack fp))
                     ( "secureAuth", b ) ->
                         SecureAuth <$> parseBool b
                     ( "reportDataTruncation", b ) ->
                         ReportDataTruncation <$> parseBool b
                     ( "reconnect", b ) ->
                         Reconnect <$> parseBool b
                     -- ( "sslVerifyServerCert", b) -> SSLVerifyServerCert <$> parseBool b
                     ( "foundRows", _ ) -> pure FoundRows
                     ( "ignoreSIGPIPE", _ ) -> pure IgnoreSIGPIPE
                     ( "ignoreSpace", _ ) -> pure IgnoreSpace
                     ( "interactive", _ ) -> pure Interactive
                     ( "localFiles", _ ) -> pure LocalFiles
                     ( "multiResults", _ ) -> pure MultiResults
                     ( "multiStatements", _ ) -> pure MultiStatements
                     ( "noSchema", _ ) -> pure NoSchema
                     _ -> Nothing

                 connInfo = ConnectInfo
                          { connectHost = host, connectPort = port
                          , connectUser = user, connectPassword = pw
                          , connectDatabase = db, connectOptions = options
                          , connectPath = "", connectSSL = Nothing }
             in connect connInfo >>= \hdl -> pure (hdl, close hdl))

#define FROM_BACKEND_ROW(ty) instance FromBackendRow MySQL ty

FROM_BACKEND_ROW(Bool)
FROM_BACKEND_ROW(Word)
FROM_BACKEND_ROW(Word8)
FROM_BACKEND_ROW(Word16)
FROM_BACKEND_ROW(Word32)
FROM_BACKEND_ROW(Word64)
FROM_BACKEND_ROW(Int)
FROM_BACKEND_ROW(Int8)
FROM_BACKEND_ROW(Int16)
FROM_BACKEND_ROW(Int32)
FROM_BACKEND_ROW(Int64)
FROM_BACKEND_ROW(Float)
FROM_BACKEND_ROW(Double)
FROM_BACKEND_ROW(Scientific)
FROM_BACKEND_ROW((Ratio Integer))
FROM_BACKEND_ROW(BS.ByteString)
FROM_BACKEND_ROW(BL.ByteString)
FROM_BACKEND_ROW(T.Text)
FROM_BACKEND_ROW(TL.Text)
FROM_BACKEND_ROW(Day)
FROM_BACKEND_ROW(LocalTime)
FROM_BACKEND_ROW(A.Value)
FROM_BACKEND_ROW(SqlNull)

-- * Equality checks
#define HAS_MYSQL_EQUALITY_CHECK(ty)                       \
  instance HasSqlEqualityCheck MySQL (ty); \
  instance HasSqlQuantifiedEqualityCheck MySQL (ty);

HAS_MYSQL_EQUALITY_CHECK(Bool)
HAS_MYSQL_EQUALITY_CHECK(Double)
HAS_MYSQL_EQUALITY_CHECK(Float)
HAS_MYSQL_EQUALITY_CHECK(Int)
HAS_MYSQL_EQUALITY_CHECK(Int8)
HAS_MYSQL_EQUALITY_CHECK(Int16)
HAS_MYSQL_EQUALITY_CHECK(Int32)
HAS_MYSQL_EQUALITY_CHECK(Int64)
HAS_MYSQL_EQUALITY_CHECK(Integer)
HAS_MYSQL_EQUALITY_CHECK(Word)
HAS_MYSQL_EQUALITY_CHECK(Word8)
HAS_MYSQL_EQUALITY_CHECK(Word16)
HAS_MYSQL_EQUALITY_CHECK(Word32)
HAS_MYSQL_EQUALITY_CHECK(Word64)
HAS_MYSQL_EQUALITY_CHECK(T.Text)
HAS_MYSQL_EQUALITY_CHECK(TL.Text)
HAS_MYSQL_EQUALITY_CHECK([Char])
HAS_MYSQL_EQUALITY_CHECK(Scientific)
HAS_MYSQL_EQUALITY_CHECK(Day)
HAS_MYSQL_EQUALITY_CHECK(TimeOfDay)
HAS_MYSQL_EQUALITY_CHECK(NominalDiffTime)
HAS_MYSQL_EQUALITY_CHECK(LocalTime)

instance HasQBuilder MySQL where
    buildSqlQuery = buildSql92Query' True


-- https://dev.mysql.com/doc/refman/5.6/en/information-functions.html#function_last-insert-id
--
-- The ID that was generated is maintained in the server on a per-connection basis.
-- This means that the value returned by the function to a given client is the first AUTO_INCREMENT value generated
--   for most recent statement affecting an AUTO_INCREMENT column by that client.
-- This value cannot be affected by other clients,
--   even if they generate AUTO_INCREMENT values of their own.
-- This behavior ensures that each client can retrieve its own ID without concern for the activity of other clients,
--   and without the need for locks or transactions.

-- https://dev.mysql.com/doc/refman/8.0/en/create-temporary-table.html
--
-- A TEMPORARY table is visible only within the current session, and is dropped automatically when the session is closed.
-- This means that two different sessions can use the same temporary table name without conflicting with each other
--   or with an existing non-TEMPORARY table of the same name.
-- (The existing table is hidden until the temporary table is dropped.)

runInsertReturningList
  :: FromBackendRow MySQL (table Identity)
  => SqlInsert MySQL table
  -> MySQLM [ table Identity ]
runInsertReturningList SqlInsertNoRows = pure []
runInsertReturningList (SqlInsert _ is@(MysqlInsertSyntax tn@(MysqlTableNameSyntax shema table) fields values)) =
  case values of
    MysqlInsertSelectSyntax _    -> fail "Not implemented runInsertReturningList part handling: INSERT INTO .. SELECT .."
    MysqlInsertValuesSyntax vals -> do

      let tableB  = emit $ TE.encodeUtf8Builder $ table
      let schemaB = emit $ TE.encodeUtf8Builder $ maybe "DATABASE()" (\s -> "'" <> s <> "'") shema

      (keycols :: [T.Text]) <- runReturningList $ MysqlCommandSyntax $
        emit "SELECT `column_name` FROM `information_schema`.`columns` WHERE " <>
        emit "`table_schema`=" <> schemaB <> emit " AND `table_name`='" <> tableB <>
        emit "' AND `column_key` LIKE 'PRI'"

      let pk = intersect keycols fields

      when (null pk) $ fail "Table PK is not part of beam-table. Tables with no PK not allowed."

      (aicol :: Maybe T.Text) <- runReturningOne $ MysqlCommandSyntax $
        emit "SELECT `column_name` FROM `information_schema`.`columns` WHERE " <>
        emit "`table_schema`=" <> schemaB <> emit " AND `table_name`='" <> tableB <>
        emit "' AND `extra` LIKE 'auto_increment'"

      let equalTo :: (T.Text, MysqlExpressionSyntax) -> MysqlSyntax
          equalTo (f, v) = mysqlIdentifier f <> emit "=" <> fromMysqlExpression v

      let csfields = mysqlSepBy (emit ", ") $ fmap mysqlIdentifier fields

      let fast = do
            runNoReturn $ MysqlCommandSyntax $ fromMysqlInsert is

            -- Select inserted rows by Primary Keys
            -- Result can be totally wrong if some of (vals :: MysqlExpressionSyntax) can result in
            -- different values when evaluated by db.
            runReturningList $ MysqlCommandSyntax $ emit "SELECT " <> csfields <> emit " FROM " <> fromMysqlTableName tn <> emit " WHERE " <>
              mysqlSepBy (emit " OR ") (mysqlSepBy (emit " AND ") . fmap equalTo . filter (flip elem pk . fst) . zip fields <$> vals)

      case aicol of
        Nothing -> fast -- no AI we can use PK to select inserted rows.
        Just ai -> if not $ elem ai pk
          then fast     -- AI exists and not part of PK, so we don't care about it
          else do       -- AI exists and is part of PK
            let tempTableName = emit "`_insert_returning_implementation`"

            runNoReturn $ MysqlCommandSyntax $
              emit "DROP TEMPORARY TABLE IF EXISTS " <> tempTableName

            runNoReturn $ MysqlCommandSyntax $
              emit "CREATE TEMPORARY TABLE " <> tempTableName <> emit " SELECT " <> csfields <> emit " FROM " <> fromMysqlTableName tn <> emit " LIMIT 0"

            flip mapM_ vals $ \val -> do
              runNoReturn $ MysqlCommandSyntax $
                fromMysqlInsert $ MysqlInsertSyntax tn fields (MysqlInsertValuesSyntax [val])

              -- hacky. But is there any other way to figure out if AI field is set to some value, or DEFAULT, for example?
              let compareMysqlExporessions a b =
                    (toLazyByteString $ unwrapInnerBuilder $ fromMysqlExpression a) ==
                    (toLazyByteString $ unwrapInnerBuilder $ fromMysqlExpression b)

              let go (f, v) = (f, if f == ai && compareMysqlExporessions v defaultE then MysqlExpressionSyntax $ emit "LAST_INSERT_ID()" else v)

              -- Select inserted rows by Primary Keys
              -- Result can be totally wrong if some of (vals :: MysqlExpressionSyntax) can result in
              -- different values when evaluated by db.
              runNoReturn $ MysqlCommandSyntax $
                emit "INSERT INTO " <> tempTableName <> emit " SELECT " <> csfields <> emit " FROM " <> fromMysqlTableName tn <>
                emit " WHERE " <> (mysqlSepBy (emit " AND ") $ fmap equalTo $ filter (flip elem pk . fst) $ map go $ zip fields val)

            res <- runReturningList $ MysqlCommandSyntax $
              emit "SELECT " <> csfields <> emit " FROM " <> tempTableName

            runNoReturn $ MysqlCommandSyntax $
              emit "DROP TEMPORARY TABLE " <> tempTableName

            pure res


instance Beam.MonadBeamInsertReturning MySQL MySQLM where
  runInsertReturningList = runInsertReturningList

runInsertRowReturning
  :: FromBackendRow MySQL (table Identity)
  => SqlInsert MySQL table
  -> MySQLM (Maybe (table Identity))
runInsertRowReturning SqlInsertNoRows = pure Nothing
runInsertRowReturning (SqlInsert _ is@(MysqlInsertSyntax tn@(MysqlTableNameSyntax schema table) fields values)) =
  case values of
    MysqlInsertSelectSyntax _    -> fail "Not implemented runInsertReturningList part handling: INSERT INTO .. SELECT .."
    MysqlInsertValuesSyntax (_:_:_) -> fail "runInsertRowReturning can't be used to insert several rows"
    MysqlInsertValuesSyntax ([]) -> pure Nothing
    MysqlInsertValuesSyntax ([vals]) -> do
      let tableB  = emit $ TE.encodeUtf8Builder $ table
      let schemaB = emit $ TE.encodeUtf8Builder $ maybe "DATABASE()" (\s -> "'" <> s <> "'") schema

      (keycols :: [T.Text]) <- runReturningList $ MysqlCommandSyntax $
        emit "SELECT `column_name` FROM `information_schema`.`columns` WHERE " <>
        emit "`table_schema`=" <> schemaB <> emit " AND `table_name`='" <> tableB <>
        emit "' AND `column_key` LIKE 'PRI'"

      let primaryKeyCols = intersect keycols fields

      when (null primaryKeyCols) $ fail "Table PK is not part of beam-table. Tables with no PK not allowed."

      (mautoIncrementCol :: Maybe T.Text) <- runReturningOne $ MysqlCommandSyntax $
        emit "SELECT `column_name` FROM `information_schema`.`columns` WHERE " <>
        emit "`table_schema`=" <> schemaB <> emit " AND `table_name`='" <> tableB <>
        emit "' AND `extra` LIKE 'auto_increment'"

      let equalTo :: (T.Text, MysqlExpressionSyntax) -> MysqlSyntax
          equalTo (field, value) = mysqlIdentifier field <> emit "=" <> fromMysqlExpression value

      let fieldsExpr = mysqlSepBy (emit ", ") $ fmap mysqlIdentifier fields

      let selectByPrimaryKeyCols colValues =
            -- Select inserted rows by Primary Keys
            -- Result can be totally wrong if some of (vals :: MysqlExpressionSyntax) can result in
            -- different values when evaluated by db.
            runReturningOne $ MysqlCommandSyntax $
              emit "SELECT " <> fieldsExpr <> emit " FROM " <> fromMysqlTableName tn <> emit " WHERE " <>
              (mysqlSepBy (emit " AND ") . fmap equalTo . filter ((`elem` primaryKeyCols) . fst) $ colValues)

      let insertReturningWithoutAutoincrement = do
            runNoReturn $ MysqlCommandSyntax $ fromMysqlInsert is
            selectByPrimaryKeyCols $ zip fields vals

      case mautoIncrementCol of
        Nothing -> insertReturningWithoutAutoincrement -- no AI we can use PK to select inserted rows.
        Just aiCol ->
          if notElem aiCol primaryKeyCols
          then insertReturningWithoutAutoincrement    -- AI exists and not part of PK, so we don't care about it
          else do                                     -- AI exists and is part of PK
            runNoReturn $ MysqlCommandSyntax $
              fromMysqlInsert $ MysqlInsertSyntax tn fields (MysqlInsertValuesSyntax [vals])

            -- hacky. But is there any other way to figure out if AI field is set to some value, or DEFAULT, for example?
            let compareMysqlExpressions a b =
                  (toLazyByteString $ unwrapInnerBuilder $ fromMysqlExpression a) ==
                  (toLazyByteString $ unwrapInnerBuilder $ fromMysqlExpression b)

            let compareWithAutoincrement (field, value) =
                  (field, if field == aiCol && compareMysqlExpressions value defaultE
                          then MysqlExpressionSyntax $ emit "last_insert_id()"
                          else value)

            selectByPrimaryKeyCols . map compareWithAutoincrement $ zip fields vals
