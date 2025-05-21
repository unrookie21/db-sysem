package dbSystem;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

public class Record {

    private byte nullBitMap;
    private LinkedHashMap<String, String> recordMap = new LinkedHashMap<>();
    private Integer pointerField;

    // 기본 생성자
    public Record(){


    }

    public byte getNullBitMap() {
        return nullBitMap;
    }

    public void setNullBitMap(byte nullBitMap) {
        this.nullBitMap = nullBitMap;
    }

    public LinkedHashMap<String, String> getRecordMap() {
        return recordMap;
    }

    public void setRecordMap(LinkedHashMap<String, String> recordMap) {
        this.recordMap = recordMap;
    }

    public Integer getPointerField() {
        return pointerField;
    }

    public void setPointerField(Integer pointerField) {
        this.pointerField = pointerField;
    }
}
