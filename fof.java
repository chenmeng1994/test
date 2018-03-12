import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class fof {
    public static void main(String[] args) throws Exception {
        BufferedReader bufferedReader=new BufferedReader(new InputStreamReader(new FileInputStream("F:\\friends.txt")));
        List<Score> scores=new ArrayList<Score>();
        String s=null;
        List<char[]> l=new ArrayList<char[]>();
        Map<Character,Integer> map=new HashMap<Character, Integer>();
        while((s=bufferedReader.readLine())!=null){
            char[] c = s.replace(" ", "").toCharArray();
            l.add(c);
            map.put(c[0],c.length-1);
        }
        for(int i=0;i<l.size();i++){
            for(int j=0;j<l.size();j++){
                if(i!=j){
                    Score score = aaa(l.get(i), l.get(j));
                    if(score!=null){
                        List<Character> l1 = score.getL();
                        double wscore=0.0;
                        for(char c:l1){
                            double sqrt = Math.sqrt(map.get(c));
                            double v = 1 / sqrt;
                            wscore+=v;
                        }
                        double v = wscore / score.getUnion();//得出加权的分数
                        score.setWscore(v);
                        scores.add(score);

                    }
                }
            }
        }
        for(Score score:scores){
            System.out.println(score);
        }
    }
    //此方法得出两个用户之间的关系类
    public static Score aaa(char[] a,char[] b){
        char x=a[0];
        //直接好友返回null
        for(char y:b){
            if(x==y){
                return null;
            }
        }
        List<Character> l=new ArrayList<Character>();

        for(int i=1;i<a.length;i++){
            for(int j=1;j<b.length;j++){
                if(a[i]==b[j]){
                    l.add(a[i]);
                }
            }
        }
        if(l.size()==0){
            return null;//没有共同好友返回null
        }
        Score score = new Score();
        score.setL(l);
        score.setA(a[0]);
        score.setB(b[0]);
        score.setIntersection(l.size());
        score.setUnion(a.length+b.length-l.size()-2);
        return score;
    }
}
