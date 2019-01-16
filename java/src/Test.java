import java.util.Calendar;
import java.util.Date;

public class Test {
    public static void main(String[] args) {
        ThreadLocal threadLocal=new ThreadLocal();
        Thread thread=new Thread();



        Date d = new Date();
        // 在默认时区下输出日期和时间值
        System.out.println(d);


        System.out.println(Calendar.getInstance().getTime());
    }

}

