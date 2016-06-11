package demo;

import org.bson.BasicBSONObject;
import org.yaml.snakeyaml.Yaml;
import org.yaml.snakeyaml.constructor.Constructor;
import org.yaml.snakeyaml.constructor.AbstractConstruct;
import org.yaml.snakeyaml.nodes.Node;
import org.yaml.snakeyaml.nodes.SequenceNode;
import org.yaml.snakeyaml.nodes.Tag;

import java.io.File;
import java.io.FileInputStream;
import java.util.List;
import java.util.Map;

/**
 * Created by chao on 9/12/15.
 * This file is used to load parameters from a YAML file.
 */
public class Config {

    class DiceConstructor extends Constructor {

        public DiceConstructor() {
            this.yamlConstructors.put(new Tag("!join"), new ConstructDice());
        }

        private class ConstructDice extends AbstractConstruct {
            public Object construct(Node node) {
                List val = (List) constructSequence((SequenceNode) node);
                String ret = "";
                for (int i=0; i < val.size(); i++) {
                    ret += (String) val.get(i);
                }
                return ret;
            }
        }

    }

    public Map load(String paraFile) throws Exception {
        Yaml yaml = new Yaml(new DiceConstructor());
        return (Map) yaml.load(new FileInputStream(new File(paraFile)));
    }

    /** ---------------------------------- main ---------------------------------- **/
    public static void main(String [] args) throws Exception {
        String paraFile = "../run/nyc_sample.yaml";
        Config config = new Config();
        Map para = config.load(paraFile);
        System.out.println(para);
        System.out.println(new BasicBSONObject(para));
    }

}
