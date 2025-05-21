package dbSystem;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

public class FileUtil {

    // 새로운 txt 파일을 생성하는 메소드.
    public static void createTxtFile(String fileName) {

        Path path = Paths.get(fileName);
        try{
            Files.write(path, new byte[0]);
            System.out.println("파일 생성 성공");
        } catch(IOException e){
            e.printStackTrace();
            System.out.println("파일 생성 실패 ");
        }

    }

}
