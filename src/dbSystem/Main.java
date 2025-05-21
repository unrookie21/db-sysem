package dbSystem;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.*;
import java.util.*;

import static dbSystem.DbConst.*;

import javax.swing.plaf.nimbus.State;

public class Main {
    private static Connection con = null;
    public static Scanner scanner = new Scanner(System.in);

    // 블록 크기 상수 정의
    private static final int BLOCK_SIZE = 40;

    private static byte[] readBlock(RandomAccessFile raf, long blockOffset) throws IOException {
        byte[] blockData = new byte[BLOCK_SIZE];
        raf.seek(blockOffset);
        raf.read(blockData);
        return blockData;
    }

    private static void writeBlock(RandomAccessFile raf, long blockOffset, byte[] blockData) throws IOException {
        raf.seek(blockOffset);
        raf.write(blockData);
    }

    /**
     * 블록 내의 특정 위치에서 int 값을 읽는 메소드
     */
    private static int getIntFromBlock(byte[] blockData, int offset) {
        return ((blockData[offset] & 0xFF) << 24) |
                ((blockData[offset + 1] & 0xFF) << 16) |
                ((blockData[offset + 2] & 0xFF) << 8) |
                (blockData[offset + 3] & 0xFF);
    }

    /**
     * 블록 내의 특정 위치에 int 값을 쓰는 메소드
     */
    private static void putIntToBlock(byte[] blockData, int offset, int value) {
        blockData[offset] = (byte) (value >> 24);
        blockData[offset + 1] = (byte) (value >> 16);
        blockData[offset + 2] = (byte) (value >> 8);
        blockData[offset + 3] = (byte) value;
    }

    public static Connection getConnection() throws SQLException {
        return DriverManager.getConnection(DbConst.DB_URL, DbConst.USER, DbConst.PASS);
    }

    // 순차파일 생성
    private static void createSequentialFile() {
        System.out.println("파일 이름, 칼럼 이름, 칼럼 길이를 입력해주세요 : ");
        String inputText = scanner.next();

        String[] splitResult = inputText.split(",");
        String fileName = splitResult[0] + ".txt";

        LinkedHashMap<String, Integer> map = new LinkedHashMap<>();
        int half = (splitResult.length - 1) / 2;

        System.out.println("필드 구성:");
        for (int i = 1; i <= half; i++) {
            String key = splitResult[i];
            int value = Integer.parseInt(splitResult[i + half]);
            map.put(key, value);
            System.out.println(i + ". " + key + " (길이: " + value + ")");
        }

        // 파일 생성하기
        FileUtil.createTxtFile(fileName);

        // 블록 I/O로 헤더 블록 초기화
        try (RandomAccessFile raf = new RandomAccessFile(fileName, "rw")) {
            // 헤더 블록 생성 (40바이트)
            byte[] headerBlock = new byte[BLOCK_SIZE];

            // 첫 4바이트에 -1 삽입 (첫 레코드 오프셋)
            putIntToBlock(headerBlock, 0, -1);

            // 헤더 블록 쓰기
            writeBlock(raf, 0, headerBlock);

            System.out.println("헤더 블록 초기화 성공");

        } catch (IOException e) {
            throw new RuntimeException("헤더 블록 초기화 실패", e);
        }

        // LinkedHashMap 기반으로 테이블 생성
        try {
            con = getConnection();
            Statement stmt = con.createStatement();

            // 기존 테이블 삭제
            String tmp = "DROP TABLE IF EXISTS " + splitResult[0];
            stmt.execute(tmp);

            // 테이블 생성
            String sql = createTableSql(splitResult[0], map);
            stmt.execute(sql);

            System.out.println("테이블 생성 완료");
            System.out.println("첫 번째 필드 '" + splitResult[1] + "'가 자동으로 search key로 사용됩니다.");

        } catch (SQLException e) {
            System.out.println("테이블 생성 실패: " + e.getMessage());
        }
    }

    // SQL 생성 메소드
    private static String createTableSql(String fileName, HashMap<String, Integer> map) {
        LinkedHashMap<String, Integer> orderedMap = new LinkedHashMap<>(map);

        StringBuilder sql = new StringBuilder("create table " + fileName + " (");
        int size = map.size();
        int index = 1;

        for (Map.Entry<String, Integer> entry : orderedMap.entrySet()) {
            sql.append(entry.getKey()).append(" char(").append(entry.getValue()).append(")");
            if (index++ < size) sql.append(",");
        }
        sql.append(")");

        System.out.println("생성 SQL: " + sql);
        return sql.toString();
    }

    // 정렬 삽입으로 변경된 레코드 삽입 메소드
    public static void insertRecord() {
        System.out.println("레코드를 삽입할 대상 파일을 입력하세요");
        String targetFile = scanner.next(); // ex. f1

        // 테이블의 첫 번째 컬럼을 자동으로 search key로 설정
        String searchKeyField = getFirstColumnName(targetFile);
        // ex. searchKeyField = A
        if (searchKeyField == null) {
            System.out.println("테이블 메타데이터를 읽을 수 없습니다.");
            return;
        }

        System.out.println("Search Key로 '" + searchKeyField + "' 필드를 사용합니다.");

        System.out.println("삽입할 레코드의 개수를 선택하세요");
        int count = scanner.nextInt();

        // 각 레코드 삽입
        for (int i = 0; i < count; i++) {
            System.out.println("레코드 " + (i+1) + " 입력 (세미콜론으로 필드 구분, 예: 00001;John;A)");
            String inputRecord = scanner.next(); // 사용자가 입력한 레코드 스트링


            // 레코드 포맷팅
            Record formatted = recordFormatting(targetFile, inputRecord);

            // search key 기반으로 정렬된 위치에 삽입
            insertRecordSorted(targetFile, formatted, searchKeyField);

            // 위과정은 파일에 삽입하는 과정이었음. 이제, 실제 db 에도 insert 문 날린다.
            try{
                con = getConnection();
                Statement stmt = con.createStatement();
                StringBuilder sql = new StringBuilder("insert into " + targetFile + " values(");

                LinkedHashMap<String, String> recordMap = formatted.getRecordMap();
                int j = 0;

                for (Map.Entry<String, String> entry : recordMap.entrySet()) {
                    String value = entry.getValue();
                    // null 처리
                    if (value == null){
                        sql.append("NULL");
                    } else {
                        sql.append("'").append(value).append("'");
                    }

                    if (j < recordMap.size() - 1){
                        sql.append(", ");
                    }

                    j++;
                }

                sql.append(")");

                System.out.println("실행할 SQL: " + sql.toString());
                stmt.executeUpdate(sql.toString());
                System.out.println("DB 테이블에 레코드 삽입 완료");


            } catch (SQLException e) {
                e.printStackTrace();
            }

        }
    }

    /**
     * 테이블의 첫 번째 컬럼 이름을 반환
     */
    private static String getFirstColumnName(String tableName) {
        try {
            Connection con = getConnection();
            DatabaseMetaData meta = con.getMetaData();
            ResultSet rs = meta.getColumns(null, schema, tableName, "%");

            // ORDINAL_POSITION이 1인 첫 번째 컬럼 찾기
            String firstName = null;
            int lowestPosition = Integer.MAX_VALUE;

            while (rs.next()) {
                String colName = rs.getString("COLUMN_NAME");
                int position = rs.getInt("ORDINAL_POSITION");

                // 가장 작은 ORDINAL_POSITION을 가진 컬럼 찾기
                if (position < lowestPosition) {
                    lowestPosition = position;
                    firstName = colName;
                }
            }
            rs.close();

            return firstName;
        } catch (SQLException e) {
            System.out.println("테이블 메타데이터 조회 중 오류: " + e.getMessage());
            return null;
        }
    }

    /**
     * Search Key 기반으로 정렬된 위치에 레코드를 삽입
     */
    private static void insertRecordSorted(String targetFile, Record record, String searchKeyField) {
        // 삽입하려는 레코드의 search key 값 가져오기
        String newKeyValue = record.getRecordMap().get(searchKeyField);
        if (newKeyValue == null) {
            System.out.println("경고: 삽입할 레코드에 search key가 null입니다.");
            newKeyValue = "";  // 빈 문자열은 모든 값보다 작게 비교됨
        }

        System.out.println("삽입할 레코드의 " + searchKeyField + " 값: " + newKeyValue);

        // 레코드 바이트 구성
        byte[] recordBytes = buildRecordBytes(targetFile, record);
        System.out.println("레코드 크기: " + recordBytes.length + " 바이트");

        String fileName = targetFile + ".txt";
        try (RandomAccessFile raf = new RandomAccessFile(fileName, "rw")) {
            if (raf.length() < BLOCK_SIZE) {
                throw new IOException("파일이 너무 작습니다.");
            }

            // 블록 I/O로 헤더 블록 읽기
            byte[] headerBlock = readBlock(raf, 0);
            int firstRecOffset = getIntFromBlock(headerBlock, 0);

            // 파일에 아직 레코드가 없는 경우
            if (firstRecOffset == -1) {
                // 첫 번째 레코드 삽입
                long newRecOffset = writeRecordToBlock(raf, recordBytes);

                // 헤더 블록 업데이트
                putIntToBlock(headerBlock, 0, (int)newRecOffset);
                writeBlock(raf, 0, headerBlock);

                // 포인터 필드는 0 (마지막 레코드)
                writePointerField(raf, newRecOffset, targetFile, 0);

                System.out.println("첫 번째 레코드 삽입 완료, 오프셋: " + newRecOffset);
                return;
            }

            // search key 필드의 인덱스 찾기
            int searchKeyIndex = findFieldIndex(targetFile, searchKeyField);
            if (searchKeyIndex == -1) {
                throw new RuntimeException("테이블에 '" + searchKeyField + "' 필드가 없습니다.");
            }

            // 삽입 위치 찾기
            long prevOffset = -1;
            long currOffset = firstRecOffset;
            boolean positionFound = false;
            Set<Long> visitedOffsets = new HashSet<>();

            while (currOffset != 0 && !positionFound) {
                if (visitedOffsets.contains(currOffset)) {
                    System.out.println("경고: 포인터 체인에 순환 참조 감지됨!");
                    break;
                }
                visitedOffsets.add(currOffset);

                // 현재 레코드의 search key 값 읽기
                String currKeyValue = readFieldValue(raf, currOffset, searchKeyIndex, targetFile);

                if (currKeyValue != null && currKeyValue.compareTo(newKeyValue) >= 0) {
                    positionFound = true;
                } else {
                    prevOffset = currOffset;
                    int nextOffset = readPointerField(raf, currOffset, targetFile);
                    currOffset = nextOffset;
                }
            }

            // 레코드 파일에 기록
            long newRecOffset = writeRecordToBlock(raf, recordBytes);

            // 포인터 체인 업데이트
            if (prevOffset == -1) {
                // 첫 번째 레코드 앞에 삽입
                writePointerField(raf, newRecOffset, targetFile, firstRecOffset);

                // 헤더 블록 업데이트
                putIntToBlock(headerBlock, 0, (int)newRecOffset);
                writeBlock(raf, 0, headerBlock);

                System.out.println("첫 번째 레코드 앞에 삽입 완료, 오프셋: " + newRecOffset);
            } else if (currOffset == 0) {
                // 마지막 레코드 뒤에 삽입
                writePointerField(raf, prevOffset, targetFile, (int)newRecOffset);
                writePointerField(raf, newRecOffset, targetFile, 0);
                System.out.println("마지막 레코드 뒤에 삽입 완료, 오프셋: " + newRecOffset);
            } else {
                // 중간에 삽입
                writePointerField(raf, prevOffset, targetFile, (int)newRecOffset);
                writePointerField(raf, newRecOffset, targetFile, (int)currOffset);
                System.out.println("중간에 삽입 완료, 오프셋: " + newRecOffset);
            }

        } catch (IOException | SQLException e) {
            throw new RuntimeException("정렬 삽입 중 오류 발생: " + e.getMessage(), e);
        }
    }

    /**
     * 블록 I/O 방식으로 레코드 읽기
     */
    private static String readFieldValue(RandomAccessFile raf, long recordOffset, int fieldIndex, String tableName)
            throws IOException, SQLException {
        // 해당 레코드가 어느 블록에 있는지 계산
        long blockOffset = (recordOffset / BLOCK_SIZE) * BLOCK_SIZE;
        int offsetInBlock = (int)(recordOffset % BLOCK_SIZE);

        // 블록 읽기
        byte[] block = readBlock(raf, blockOffset);

        // Null 비트맵 읽기
        byte nullBitMap = block[offsetInBlock];

        // null 여부 확인
        int bitIndex = 7 - fieldIndex;
        boolean isNull = ((nullBitMap >> bitIndex) & 1) == 1;

        if (isNull) {
            return null;
        }

        // 필드 위치 계산을 위해 메타데이터 가져오기
        List<Integer> fieldSizes = getFieldSizes(tableName);

        // 필드 위치 계산
        int fieldOffset = offsetInBlock + 1; // nullBitMap 다음부터

        for (int i = 0; i < fieldIndex; i++) {
            int precedingBitIndex = 7 - i;
            boolean precedingFieldIsNull = ((nullBitMap >> precedingBitIndex) & 1) == 1;

            if (!precedingFieldIsNull) {
                fieldOffset += fieldSizes.get(i);
            }
        }

        // 필드가 블록 경계를 넘는지 확인
        int fieldSize = fieldSizes.get(fieldIndex);

        if (fieldOffset + fieldSize <= BLOCK_SIZE) {
            // 필드가 현재 블록 내에 있는 경우
            byte[] fieldData = Arrays.copyOfRange(block, fieldOffset, fieldOffset + fieldSize);
            return new String(fieldData).trim();
        } else {
            // 필드가 블록 경계를 넘는 경우
            ByteArrayOutputStream bos = new ByteArrayOutputStream();

            // 현재 블록에서 읽을 수 있는 만큼 읽기
            int bytesInCurrentBlock = BLOCK_SIZE - fieldOffset;
            bos.write(block, fieldOffset, bytesInCurrentBlock);

            // 다음 블록에서 나머지 읽기
            byte[] nextBlock = readBlock(raf, blockOffset + BLOCK_SIZE);
            bos.write(nextBlock, 0, fieldSize - bytesInCurrentBlock);

            return new String(bos.toByteArray()).trim();
        }
    }

    /**
     * 테이블의 필드 크기 목록 가져오기
     */
    private static List<Integer> getFieldSizes(String tableName) throws SQLException {
        Connection con = getConnection();
        DatabaseMetaData meta = con.getMetaData();
        ResultSet rs = meta.getColumns(null, schema, tableName, "%");

        Map<Integer, Integer> sizesByPosition = new TreeMap<>();
        while (rs.next()) {
            int position = rs.getInt("ORDINAL_POSITION");
            int size = rs.getInt("COLUMN_SIZE");
            sizesByPosition.put(position, size);
        }
        rs.close();

        return new ArrayList<>(sizesByPosition.values());
    }

    /**
     * 블록 I/O 방식으로 포인터 필드 읽기
     */
    private static int readPointerField(RandomAccessFile raf, long recordOffset, String tableName)
            throws IOException, SQLException {
        // 포인터 위치 계산
        long pointerPos = findPointerPos(raf, recordOffset, tableName);

        // 포인터가 위치한 블록 계산
        long blockOffset = (pointerPos / BLOCK_SIZE) * BLOCK_SIZE;
        int offsetInBlock = (int)(pointerPos % BLOCK_SIZE);

        // 블록 읽기
        byte[] block = readBlock(raf, blockOffset);

        // 블록 경계를 넘는지 확인
        if (offsetInBlock + 4 <= BLOCK_SIZE) {
            // 포인터가 현재 블록 내에 있는 경우
            return getIntFromBlock(block, offsetInBlock);
        } else {
            // 포인터가 블록 경계를 넘는 경우
            ByteBuffer buffer = ByteBuffer.allocate(4);

            // 현재 블록에서 읽을 수 있는 만큼 읽기
            int bytesInCurrentBlock = BLOCK_SIZE - offsetInBlock;
            buffer.put(Arrays.copyOfRange(block, offsetInBlock, BLOCK_SIZE));

            // 다음 블록에서 나머지 읽기
            byte[] nextBlock = readBlock(raf, blockOffset + BLOCK_SIZE);
            buffer.put(nextBlock, 0, 4 - bytesInCurrentBlock);

            buffer.flip();
            return buffer.getInt();
        }
    }

    /**
     * 블록 I/O 방식으로 포인터 필드 위치 찾기
     */
    private static long findPointerPos(RandomAccessFile raf, long recordOffset, String tableName)
            throws IOException, SQLException {
        // 레코드가 위치한 블록 계산
        long blockOffset = (recordOffset / BLOCK_SIZE) * BLOCK_SIZE;
        int offsetInBlock = (int)(recordOffset % BLOCK_SIZE);

        // 블록 읽기
        byte[] block = readBlock(raf, blockOffset);

        // Null 비트맵 읽기
        byte nullBitMap = block[offsetInBlock];

        // 필드 크기 목록 가져오기
        List<Integer> fieldSizes = getFieldSizes(tableName);

        // 포인터 위치 계산
        long pointerPos = recordOffset + 1; // nullBitMap 다음부터

        for (int i = 0; i < fieldSizes.size(); i++) {
            int bitIndex = 7 - i;
            boolean isNull = ((nullBitMap >> bitIndex) & 1) == 1;

            if (!isNull) {
                pointerPos += fieldSizes.get(i);
            }
        }

        return pointerPos;
    }

    /**
     * 블록 I/O 방식으로 포인터 필드 쓰기
     */
    private static void writePointerField(RandomAccessFile raf, long recordOffset, String tableName, int newPointer)
            throws IOException, SQLException {
        // 포인터 위치 계산
        long pointerPos = findPointerPos(raf, recordOffset, tableName);

        // 포인터가 위치한 블록 계산
        long blockOffset = (pointerPos / BLOCK_SIZE) * BLOCK_SIZE;
        int offsetInBlock = (int)(pointerPos % BLOCK_SIZE);

        // 블록 읽기
        byte[] block = readBlock(raf, blockOffset);

        // 블록 경계를 넘는지 확인
        if (offsetInBlock + 4 <= BLOCK_SIZE) {
            // 포인터가 현재 블록 내에 있는 경우
            putIntToBlock(block, offsetInBlock, newPointer);
            writeBlock(raf, blockOffset, block);
        } else {
            // 포인터가 블록 경계를 넘는 경우
            ByteBuffer buffer = ByteBuffer.allocate(4);
            buffer.putInt(newPointer);
            buffer.flip();

            // 현재 블록에 일부 쓰기
            int bytesInCurrentBlock = BLOCK_SIZE - offsetInBlock;
            for (int i = 0; i < bytesInCurrentBlock; i++) {
                block[offsetInBlock + i] = buffer.get();
            }
            writeBlock(raf, blockOffset, block);

            // 다음 블록에 나머지 쓰기
            byte[] nextBlock = readBlock(raf, blockOffset + BLOCK_SIZE);
            for (int i = 0; i < 4 - bytesInCurrentBlock; i++) {
                nextBlock[i] = buffer.get();
            }
            writeBlock(raf, blockOffset + BLOCK_SIZE, nextBlock);
        }
    }

    /**
     * 블록 I/O 방식으로 레코드 쓰기
     */
    private static long writeRecordToBlock(RandomAccessFile raf, byte[] recordBytes) throws IOException {
        // 적합한 블록 찾기
        long currentBlockOffset = BLOCK_SIZE; // 첫 번째 데이터 블록 시작

        while (true) {
            // 파일 크기를 초과하면 새 블록 생성
            if (currentBlockOffset >= raf.length()) {
                byte[] newBlock = new byte[BLOCK_SIZE];
                putIntToBlock(newBlock, 0, 0); // usedSpace 초기화
                writeBlock(raf, currentBlockOffset, newBlock);
            }

            // 블록 읽기
            byte[] block = readBlock(raf, currentBlockOffset);

            // 블록의 사용 공간 읽기
            int usedSpace = getIntFromBlock(block, 0);

            // 남은 공간 계산
            int leftOver = BLOCK_SIZE - 4 - usedSpace;

            // 충분한 공간이 있으면 삽입
            if (recordBytes.length <= leftOver) {
                // 레코드 삽입 위치 계산
                long newRecOffset = currentBlockOffset + 4 + usedSpace;
                int offsetInBlock = 4 + usedSpace;

                // 블록 경계를 넘는지 확인
                if (offsetInBlock + recordBytes.length <= BLOCK_SIZE) {
                    // 현재 블록 내에 있는 경우
                    System.arraycopy(recordBytes, 0, block, offsetInBlock, recordBytes.length);
                } else {
                    // 블록 경계를 넘는 경우
                    int bytesInCurrentBlock = BLOCK_SIZE - offsetInBlock;
                    System.arraycopy(recordBytes, 0, block, offsetInBlock, bytesInCurrentBlock);

                    // 다음 블록에 나머지 쓰기
                    byte[] nextBlock = new byte[BLOCK_SIZE];
                    if (currentBlockOffset + BLOCK_SIZE < raf.length()) {
                        nextBlock = readBlock(raf, currentBlockOffset + BLOCK_SIZE);
                    }

                    System.arraycopy(recordBytes, bytesInCurrentBlock, nextBlock, 0, recordBytes.length - bytesInCurrentBlock);
                    writeBlock(raf, currentBlockOffset + BLOCK_SIZE, nextBlock);
                }

                // usedSpace 업데이트
                usedSpace += recordBytes.length;
                putIntToBlock(block, 0, usedSpace);
                writeBlock(raf, currentBlockOffset, block);

                System.out.println("블록 " + (currentBlockOffset / BLOCK_SIZE) + "에 레코드 삽입, 오프셋: " + newRecOffset);
                return newRecOffset;
            } else {
                // 다음 블록으로 이동
                currentBlockOffset += BLOCK_SIZE;
            }
        }
    }

    private static byte[] buildRecordBytes(String targetFile, Record rec) {
        // Null BitMap
        byte nullMap = rec.getNullBitMap();

        // 필드 메타데이터
        List<String> fieldNames = new ArrayList<>();
        List<Integer> fieldSizes = new ArrayList<>();

        try {
            Connection con = getConnection();
            DatabaseMetaData meta = con.getMetaData();
            ResultSet rs = meta.getColumns(null, schema, targetFile, "%");

            while (rs.next()) {
                fieldNames.add(rs.getString("COLUMN_NAME"));
                fieldSizes.add(rs.getInt("COLUMN_SIZE"));
            }
            rs.close();
        } catch (SQLException e) {
            throw new RuntimeException("메타데이터 로드 실패", e);
        }

        // 레코드 바이트 구성
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        try {
            // Null BitMap 쓰기
            bos.write(nullMap);

            // 필드 값 쓰기
            LinkedHashMap<String, String> map = rec.getRecordMap();

            for (int i = 0; i < fieldNames.size(); i++) {
                String columnName = fieldNames.get(i);
                int len = fieldSizes.get(i);
                String value = map.get(columnName);

                int bitIndex = 7 - i;
                boolean isNull = ((nullMap >> bitIndex) & 1) == 1;

                if (!isNull && value != null) {
                    String padded = padRight(value, len);
                    bos.write(padded.getBytes());
                }
            }

            // 포인터 필드 (4바이트)
            int pointer = (rec.getPointerField() == null) ? 0 : rec.getPointerField();
            DataOutputStream dos = new DataOutputStream(bos);
            dos.writeInt(pointer);

            return bos.toByteArray();
        } catch (IOException e) {
            throw new RuntimeException("레코드 바이트 변환 실패", e);
        }
    }

    // 길이 부족하면 오른쪽에 공백을 채우기
    private static String padRight(String str, int size) {
        if (str.length() >= size) return str.substring(0, size);
        return str + " ".repeat(size - str.length());
    }

    private static Record recordFormatting(String targetFile, String inputRecord) {
        Record record = new Record();
        record.setPointerField(0);

        String[] split = inputRecord.split(";");
        LinkedHashMap<String, String> recordMap = new LinkedHashMap<>();

        try {
            // 테이블 메타데이터 조회
            Map<Integer, String> columnsByPosition = new TreeMap<>();
            Connection con = getConnection();
            DatabaseMetaData metaData = con.getMetaData();
            ResultSet columnRs = metaData.getColumns(null, schema, targetFile, "%");

            while (columnRs.next()) {
                String columnName = columnRs.getString("COLUMN_NAME");
                int position = columnRs.getInt("ORDINAL_POSITION");
                columnsByPosition.put(position, columnName);
            }
            columnRs.close();

            // 입력 값 매핑
            int i = 0;
            for (String columnName : columnsByPosition.values()) {
                if (i < split.length) {
                    String val = split[i];
                    recordMap.put(columnName, val.equalsIgnoreCase("null") ? null : val);
                    i++;
                } else {
                    recordMap.put(columnName, null);
                }
            }

            record.setRecordMap(recordMap);

            // Null BitMap 생성
            StringBuilder bitString = new StringBuilder();
            for (String value : recordMap.values()) {
                bitString.append(value == null ? "1" : "0");
            }

            record.setNullBitMap(toNullBitMap(bitString.toString()));

        } catch (SQLException e) {
            throw new RuntimeException("메타데이터 로드 실패", e);
        }

        return record;
    }

    private static byte toNullBitMap(String bitString) {
        byte result = 0;

        for (int i = 0; i < bitString.length(); i++) {
            if (bitString.charAt(i) == '1') {
                result |= (1 << (7 - i));
            }
        }

        return result;
    }

    /**
     * 블록 I/O 방식으로 필드 검색
     */
    public static void searchField(String fileName, String fieldName) {
        String filePath = fileName + ".txt";
        System.out.println("🔍 " + fileName + " 파일의 " + fieldName + " 필드 검색 결과:");

        try (RandomAccessFile raf = new RandomAccessFile(filePath, "r")) {
            if (raf.length() < BLOCK_SIZE) {
                System.out.println("파일이 존재하지 않거나 너무 작습니다.");
                return;
            }

            // 헤더 블록 읽기
            byte[] headerBlock = readBlock(raf, 0);
            int firstRecOffset = getIntFromBlock(headerBlock, 0);

            if (firstRecOffset == -1) {
                System.out.println("파일에 레코드가 없습니다.");
                return;
            }

            // 필드 정보 로드
            Map<Integer, ColumnInfo> columnsByPosition = getTableColumns(fileName);
            List<String> fieldNames = new ArrayList<>();
            List<Integer> fieldSizes = new ArrayList<>();
            int targetFieldIndex = -1;
            int index = 0;

            for (Map.Entry<Integer, ColumnInfo> entry : columnsByPosition.entrySet()) {
                ColumnInfo info = entry.getValue();
                fieldNames.add(info.name);
                fieldSizes.add(info.size);

                if (info.name.equalsIgnoreCase(fieldName)) {
                    targetFieldIndex = index;
                }
                index++;
            }

            if (targetFieldIndex == -1) {
                System.out.println("'" + fieldName + "' 필드가 테이블에 존재하지 않습니다.");
                return;
            }

            // 결과 출력 헤더
            System.out.println("-------------------------------------------");
            System.out.println("| 레코드 번호 | 오프셋 | " + fieldName + " 값    |");
            System.out.println("-------------------------------------------");

            // 레코드 순회
            long currentOffset = firstRecOffset;
            int recordCount = 0;
            Set<Long> visitedOffsets = new HashSet<>();

            while (currentOffset != 0 && currentOffset != -1) {
                if (visitedOffsets.contains(currentOffset)) {
                    System.out.println("경고: 레코드 체인에 순환 참조가 감지되었습니다!");
                    break;
                }
                visitedOffsets.add(currentOffset);
                recordCount++;

                // 필드 값 읽기
                String fieldValue = readFieldValue(raf, currentOffset, targetFieldIndex, fileName);

                if (fieldValue == null) {
                    System.out.printf("| %-11d | %-6d | NULL       |\n", recordCount, currentOffset);
                } else {
                    System.out.printf("| %-11d | %-6d | %-10s |\n", recordCount, currentOffset, fieldValue);
                }

                // 다음 레코드로 이동
                int nextOffset = readPointerField(raf, currentOffset, fileName);
                currentOffset = nextOffset;
            }

            System.out.println("-------------------------------------------");
            System.out.println("총 " + recordCount + "개 레코드 검색 완료");

        } catch (IOException | SQLException e) {
            System.out.println("필드 검색 중 오류 발생: " + e.getMessage());
        }
    }

    /**
     * 블록 I/O 방식으로 범위 검색
     */
    public static List<Map<String, String>> rangeSearchRecord(String fileName, String keyField, String startValue, String endValue) {
        String filePath = fileName + ".txt";
        System.out.println("\n🔍 " + fileName + " 테이블에서 " + keyField + " 값이 '" + startValue + "'에서 '" + endValue + "'까지인 레코드 검색");

        // 검색 결과를 저장할 리스트
        List<Map<String, String>> searchResults = new ArrayList<>();

        try (RandomAccessFile raf = new RandomAccessFile(filePath, "r")) {
            if (raf.length() < BLOCK_SIZE) {
                System.out.println("파일이 존재하지 않거나 너무 작습니다.");
                return searchResults;
            }

            // 헤더 블록 읽기
            byte[] headerBlock = readBlock(raf, 0);
            int firstRecOffset = getIntFromBlock(headerBlock, 0);

            if (firstRecOffset == -1) {
                System.out.println("파일에 레코드가 없습니다.");
                return searchResults;
            }

            // 필드 정보 로드
            Map<Integer, ColumnInfo> columnsByPosition = getTableColumns(fileName);
            int keyFieldIndex = findFieldIndex(fileName, keyField);

            // 필드 이름 목록 생성
            List<String> fieldNames = new ArrayList<>();
            for (ColumnInfo info : columnsByPosition.values()) {
                fieldNames.add(info.name);
            }

            // 결과 출력 헤더
            System.out.println("\n검색 결과:");
            StringBuilder headerBuilder = new StringBuilder("| 번호 | ");
            StringBuilder separatorBuilder = new StringBuilder("|------|");

            for (String name : fieldNames) {
                headerBuilder.append(String.format(" %-15s |", name));
                separatorBuilder.append("-----------------|");
            }

            String header = headerBuilder.toString();
            String separator = separatorBuilder.toString();

            System.out.println(separator);
            System.out.println(header);
            System.out.println(separator);

            // 레코드 순회
            long currentOffset = firstRecOffset;
            int recordCount = 0;
            int resultCount = 0;
            Set<Long> visitedOffsets = new HashSet<>();

            while (currentOffset != 0 && currentOffset != -1) {
                if (visitedOffsets.contains(currentOffset)) {
                    System.out.println("경고: 레코드 체인에 순환 참조가 감지되었습니다!");
                    break;
                }
                visitedOffsets.add(currentOffset);
                recordCount++;

                // search key 값 읽기
                String keyValue = readFieldValue(raf, currentOffset, keyFieldIndex, fileName);
                // 범위 체크
                if (keyValue != null) {
                    // 정렬된 체인이므로 범위를 초과하면 검색 종료
                    if (keyValue.compareTo(endValue) > 0) {
                        break;
                    }
                    // 범위 내인 경우
                    if (keyValue.compareTo(startValue) >= 0) {
                        resultCount++;

                        // 레코드의 모든 필드 값 읽기
                        Map<String, String> recordValues = readRecordValues(raf, currentOffset, fileName);

                        // 검색 결과에 추가
                        searchResults.add(recordValues);

                        // 결과 행 출력
                        StringBuilder rowBuilder = new StringBuilder(String.format("| %-4d |", resultCount));

                        for (String name : fieldNames) {
                            String value = recordValues.getOrDefault(name, "NULL");
                            rowBuilder.append(String.format(" %-15s |", value));
                        }
                        System.out.println(rowBuilder.toString());
                    }
                }
                // 다음 레코드로 이동
                int nextOffset = readPointerField(raf, currentOffset, fileName);
                currentOffset = nextOffset;
            }

            System.out.println(separator);
            System.out.println("검색된 레코드 수: " + resultCount + " / 전체 검사한 레코드 수: " + recordCount);

        } catch (IOException | SQLException e) {
            System.out.println("범위 검색 중 오류 발생: " + e.getMessage());
        }

        // 검색 결과 반환
        return searchResults;
    }

    /**
     * 블록 I/O 방식으로 레코드의 모든 필드 값 읽기
     */
    private static Map<String, String> readRecordValues(RandomAccessFile raf, long recordOffset, String tableName)
            throws IOException, SQLException {
        // 필드 정보 가져오기
        Map<Integer, ColumnInfo> columnsByPosition = getTableColumns(tableName);

        // 필드 이름과 크기 목록 생성
        List<String> fieldNames = new ArrayList<>();
        List<Integer> fieldSizes = new ArrayList<>();

        for (ColumnInfo info : columnsByPosition.values()) {
            fieldNames.add(info.name);
            fieldSizes.add(info.size);
        }

        // 레코드가 위치한 블록 계산
        long blockOffset = (recordOffset / BLOCK_SIZE) * BLOCK_SIZE;
        int offsetInBlock = (int)(recordOffset % BLOCK_SIZE);

        // 블록 읽기
        byte[] block = readBlock(raf, blockOffset);

        // Null 비트맵 읽기
        byte nullBitMap = block[offsetInBlock];

        // 결과 맵 초기화
        Map<String, String> result = new LinkedHashMap<>();

        // 필드 값 읽기
        long fieldOffset = recordOffset + 1; // nullBitMap 다음부터

        for (int i = 0; i < fieldNames.size(); i++) {
            String fieldName = fieldNames.get(i);
            int fieldSize = fieldSizes.get(i);

            int bitIndex = 7 - i;
            boolean isNull = ((nullBitMap >> bitIndex) & 1) == 1;

            if (isNull) {
                result.put(fieldName, null);
            } else {
                String fieldValue = readFieldValue(raf, recordOffset, i, tableName);
                result.put(fieldName, fieldValue);

                // 다음 필드 오프셋 계산
                fieldOffset += fieldSize;
            }
        }

        return result;
    }

    private static Map<Integer, ColumnInfo> getTableColumns(String tableName) {
        Map<Integer, ColumnInfo> columnsByPosition = new TreeMap<>();

        try {
            Connection con = getConnection();
            DatabaseMetaData meta = con.getMetaData();
            ResultSet rs = meta.getColumns(null, schema, tableName, "%");

            while (rs.next()) {
                String colName = rs.getString("COLUMN_NAME");
                int colSize = rs.getInt("COLUMN_SIZE");
                int position = rs.getInt("ORDINAL_POSITION");

                columnsByPosition.put(position, new ColumnInfo(colName, colSize));
            }
            rs.close();

        } catch (SQLException e) {
            System.out.println("테이블 컬럼 정보 조회 중 오류: " + e.getMessage());
        }

        return columnsByPosition;
    }

    private static int findFieldIndex(String tableName, String fieldName) {
        try {
            Connection con = getConnection();
            DatabaseMetaData meta = con.getMetaData();
            ResultSet rs = meta.getColumns(null, schema, tableName, "%");

            // ORDINAL_POSITION 기준으로 정렬된 필드 목록
            Map<Integer, String> fieldsByPosition = new TreeMap<>();
            while (rs.next()) {
                String colName = rs.getString("COLUMN_NAME");
                int position = rs.getInt("ORDINAL_POSITION");
                fieldsByPosition.put(position, colName);
            }
            rs.close();

            // 필드 인덱스 찾기
            int index = 0;
            for (String colName : fieldsByPosition.values()) {
                if (colName.equalsIgnoreCase(fieldName)) {
                    return index;
                }
                index++;
            }

            return -1; // 필드 없음
        } catch (SQLException e) {
            throw new RuntimeException("필드 인덱스 조회 중 오류: " + e.getMessage(), e);
        }
    }

    static class ColumnInfo {
        String name;
        int size;

        public ColumnInfo(String name, int size) {
            this.name = name;
            this.size = size;
        }
    }

    public static void main(String[] args) {
        System.out.println("========== 순차 파일 관리 시스템 ==========");
        System.out.println("원하는 동작을 선택해주세요");
        System.out.println("1. 화일 생성 | 2. 레코드 삽입 | 3. 조인 질의");

        int menuSelect = scanner.nextInt();

        switch (menuSelect) {
            case 1:
                createSequentialFile();
                break;
            case 2:
                insertRecord();
                break;
            case 3:
                // System.out.println("검색할 파일명과 필드명을 입력하세요 (예: f1,NAME): ");
                // String input = scanner.next();
                // String[] parts = input.split(",");
                // if (parts.length == 2) {
                //     searchField(parts[0], parts[1]);
                // } else {
                //     System.out.println("잘못된 입력 형식입니다. '파일명,필드명' 형식으로 입력하세요.");
                // }

                System.out.println("merge join 으로 join 질의 결과를 확인합니다");
                System.out.println("join 의 대상이 될 테이블 두개를 입력해주세요 : ");
                String twoTable = scanner.next();
                String[] tables = twoTable.split(",");

                printJoinResult(tables[0], tables[1]);
                printSQLJoinResult(tables[0], tables[1]);

                break;
            case 4:
                // System.out.println("파일명,필드명,시작값,끝값 형식으로 입력하세요 (예: f1,ID,00001,00004): ");
                // String rangeInput = scanner.next();
                // String[] rangeParts = rangeInput.split(",");
                // if (rangeParts.length == 4) {
                //     rangeSearchRecord(rangeParts[0], rangeParts[1], rangeParts[2], rangeParts[3]);
                //     // file, key, start, end
                // } else {
                //     System.out.println("잘못된 입력 형식입니다. '파일명,필드명,시작값,끝값' 형식으로 입력하세요.");
                // }
                break;
            case 5 :


            default:
                System.out.println("잘못된 메뉴 선택입니다.");
                break;
        }
    }

    private static void printSQLJoinResult(String table1, String table2) {

        String rSearchKey = getFirstColumnName(table1);
        String sSearchKey = getFirstColumnName(table2);

        StringBuilder sql = new StringBuilder("SELECT * FROM " + table1 + "," + table2
            + " WHERE " + table1 + "." + rSearchKey + "=" + table2 + "." + sSearchKey
            + " ORDER BY " + table1 + "." + rSearchKey);

        try {
            Connection con = getConnection();
            Statement stmt = con.createStatement();

            ResultSet rs = stmt.executeQuery(sql.toString());
            ResultSetMetaData metaData = rs.getMetaData();
            int columnCount = metaData.getColumnCount();

            // 간단한 구분선 출력
            System.out.println("\n--- SQL JOIN 결과 ---");

            // 결과 행 출력
            int rowCount = 0;
            while (rs.next()) {
                rowCount++;
                StringBuilder rowBuilder = new StringBuilder(rowCount + ": ");

                for (int i = 1; i <= columnCount; i++) {
                    String value = rs.getString(i);
                    value = (value != null) ? value.trim() : "NULL";
                    rowBuilder.append(value);

                    if (i < columnCount) {
                        rowBuilder.append(", ");
                    }
                }

                System.out.println(rowBuilder.toString());
            }

            System.out.println("총 " + rowCount + "개 레코드 조회됨");

            // 리소스 해제
            rs.close();
            stmt.close();
            con.close();

        } catch (SQLException e) {
            System.out.println("SQL 조인 실행 중 오류 발생: " + e.getMessage());
            e.printStackTrace();
        }

    }

    private static void printJoinResult(String table1, String table2) {
        System.out.println("\n🔍 " + table1 + "와 " + table2 + " 테이블의 " + " Sort-Merge Join 결과");

        // 두 테이블에서 조인 키 범위 전체 검색
        // 테이블1의 searchkey
        String rSearchKey = getFirstColumnName(table1);
        String sSearchKey = getFirstColumnName(table2);
        List<Map<String, String>> rTable = rangeSearchRecord(table1, rSearchKey, "00001", "99999");
        List<Map<String, String>> sTable = rangeSearchRecord(table2, sSearchKey, "00001", "99999");

        // 결과가 없는 경우 처리
        if (rTable.isEmpty() || sTable.isEmpty()) {
            System.out.println("조인 결과가 없습니다. 하나 이상의 테이블에 레코드가 없습니다.");
            return;
        }

        // 모든 컬럼 이름 가져오기
        Set<String> allColumns = new LinkedHashSet<>();

        // 양쪽 테이블의 필드명 모두 수집 (테이블명과 함께)
        for (Map<String, String> record : rTable) {
            for (String field : record.keySet()) {
                allColumns.add(table1 + "." + field);
            }
        }

        for (Map<String, String> record : sTable) {
            for (String field : record.keySet()) {
                allColumns.add(table2 + "." + field);
            }
        }

        // 결과 테이블 헤더 출력
        System.out.println("\n조인 결과:");
        StringBuilder headerBuilder = new StringBuilder("| 번호 | ");
        StringBuilder separatorBuilder = new StringBuilder("|------|");

        for (String column : allColumns) {
            headerBuilder.append(String.format(" %-20s |", column));
            separatorBuilder.append("----------------------|");
        }

        String header = headerBuilder.toString();
        String separator = separatorBuilder.toString();

        System.out.println(separator);
        System.out.println(header);
        System.out.println(separator);

        // Sort-Merge Join 알고리즘 구현
        int joinCount = 0;
        int r = 0;  // r 포인터
        int s = 0;  // s 포인터
        Integer mark = null;  // 블록 시작 위치 표시


        while (r < rTable.size() && s < sTable.size()) {
            String rKey = rTable.get(r).get(rSearchKey);
            String sKey = sTable.get(s).get(sSearchKey);

            // null 값 처리
            if (rKey == null) {
                r++;
                continue;
            }
            if (sKey == null) {
                s++;
                continue;
            }

            // R < S인 경우 R 포인터 증가
            if (rKey.compareTo(sKey) < 0) {
                r++;
            }
            // R > S인 경우 S 포인터 증가
            else if (rKey.compareTo(sKey) > 0) {
                s++;
            }
            // R = S인 경우 (조인 조건 만족)
            else {
                // 블록 시작점 표시가 없으면 현재 S 위치 저장
                if (mark == null) {
                    mark = s;
                }

                // 조인 결과 생성 및 출력
                joinCount++;
                StringBuilder rowBuilder = new StringBuilder(String.format("| %-4d |", joinCount));

                // 조인 결과에 모든 필드 추가
                Map<String, String> joinedRecord = new LinkedHashMap<>();

                // table1의 레코드 필드
                for (Map.Entry<String, String> entry : rTable.get(r).entrySet()) {
                    joinedRecord.put(table1 + "." + entry.getKey(), entry.getValue());
                }

                // table2의 레코드 필드
                for (Map.Entry<String, String> entry : sTable.get(s).entrySet()) {
                    joinedRecord.put(table2 + "." + entry.getKey(), entry.getValue());
                }

                // 출력 행 구성
                for (String column : allColumns) {
                    String value = joinedRecord.getOrDefault(column, "NULL");
                    rowBuilder.append(String.format(" %-20s |", value));
                }
                System.out.println(rowBuilder.toString());

                // S 포인터 증가
                s++;

                // S 포인터가 끝에 도달하거나 키가 변경된 경우
                if (s >= sTable.size() || !sTable.get(s).get(sSearchKey).equals(rKey)) {
                    // R 포인터 증가
                    r++;
                    // S 포인터를 블록 시작점으로 리셋
                    s = mark;
                    // 새로운 블록의 시작을 위해 mark 초기화
                    mark = null;
                }
            }
        }

        System.out.println(separator);
        System.out.println("조인된 레코드 수: " + joinCount);
    }
}