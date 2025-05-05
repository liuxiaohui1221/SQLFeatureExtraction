package sql.encoder;

import java.util.BitSet;

public class BitSetExample {
    public static void main(String[] args) throws Exception
    {
        // 示例字符串
        String binaryString = "0101010101";

        // 创建一个BitSet对象
        BitSet bitSet = new BitSet(binaryString.length());

        // 遍历字符串中的每个字符
        for (int i = 0; i < binaryString.length(); i++) {
            // 将字符转换为对应的整数值（0或1）
            int bitValue = Character.getNumericValue(binaryString.charAt(i));

            // 如果字符是'1'，则将对应的位设置为true
            if (bitValue == 1) {
                bitSet.set(i);
            }
        }

        // 输出BitSet的内容
        System.out.println(toString(bitSet, binaryString.length()));
    }

  public static String toString(BitSet b, int size) throws Exception{
    String to_return = "";
    for(int i=0; i<size; i++) {
      if(b.get(i))
        to_return+="1";
      else
        to_return+="0";
    }
    return to_return;
  }
}
