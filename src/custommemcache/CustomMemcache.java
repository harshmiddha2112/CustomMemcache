/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package custommemcache;

import java.util.*;

/**
 *
 * @author harshmiddha
 */
public class CustomMemcache {
    static HashMap<String,String> globalHashMap=new HashMap<>();
    LocalMemcache localMemcache;
    
    public class LocalMemcache {
        public LocalMemcache localMemcache;
        public HashMap<String,String> localHashMap = new HashMap<>();
        public HashMap<String,String> deletedKeys = new HashMap<>();

        public void LocalMemcache() {
            localMemcache = new LocalMemcache();   
        }
    }

    void processInputString(String str) {
        
        String[] splited = str.split("\\s+");

        switch(splited[0]) {
            case "set":
                if (splited.length < 3) {
                    System.out.println("Invalid input");
                    break;
                }
                set(splited[1], splited[2]);
                break;
            case "delete":
                if (splited.length < 2) {
                    System.out.println("Invalid input");
                    break;
                }
                String deletedValue = delete(splited[1]);
                System.out.println(deletedValue);
                break;
            case "count":
                if (splited.length < 2) {
                    System.out.println("Invalid input");
                    break;
                }
                int count  = count(splited[1]);
                System.out.println(count);
                break;
            case "get":
                if (splited.length < 2) {
                    System.out.println("Invalid input");
                    break;
                }
                String value = get(splited[1]);
                System.out.println(value);
                break;
            case "start":
                start();
                break;
            case "commit":
                commit();
                break;
            case "rollback":
                rollback();
                break;
            default:
                System.out.println("Not a valid Operation");
        }
        
    }
 
    
    private void set(String key, String value) {
        LocalMemcache latestLocalMemcache = findLatestLocalMemcache();
        if (latestLocalMemcache == null) {
            String count  = globalHashMap.getOrDefault("count_"+value, "0");
            count = Integer.toString(Integer.parseInt(count) + 1);
            globalHashMap.put("count_"+value, count);

            if (get(key) != null) {
                int temp = count("count_"+get(key)) - 1;
                globalHashMap.put("count_"+get(key), Integer.toString(temp));
            }

            globalHashMap.put(key, value);

            System.out.println(key + " " + value);
        } else {
            String count  = latestLocalMemcache.localHashMap.getOrDefault("count_"+value, "0");
            count = Integer.toString(Integer.parseInt(count) + 1);
            latestLocalMemcache.localHashMap.put("count_"+value, count);

            if (get(key) != null) {
                int temp = count("count_"+get(key)) - 1;
                latestLocalMemcache.localHashMap.put("count_"+get(key), Integer.toString(temp));
            }

            latestLocalMemcache.localHashMap.put(key, value);

            System.out.println(key + " " + value);
        }
    }
    
    private String get(String key) {
        while(true) {
            if (localMemcache == null) {
                return globalHashMap.get(key);
            } else {
                //Storing localCache in stacks to get the latest updated value first
                Stack stack = new Stack();
                LocalMemcache latestLocalMemcache = localMemcache;
                while(latestLocalMemcache != null) {
                    stack.push(latestLocalMemcache);
                    latestLocalMemcache = latestLocalMemcache.localMemcache;
                }
                
                LocalMemcache top = (LocalMemcache) stack.pop();
                while (top != null) {
                    String value = top.localHashMap.get(key);
                    if (value != null) {
                        return value;
                    } else {
                        if(!stack.empty()) {
                            top = (LocalMemcache) stack.pop();
                        } else {
                            break;
                        }
                        
                    }
                }
                
                return globalHashMap.get(key);
                
            }
        }
        
    }
    
    private String delete(String key) {
        String value;
        while(true) {
            if (localMemcache == null) {
                value = globalHashMap.get(key);
                String count = "0";
                
                if (value != null) {
                    count = globalHashMap.getOrDefault("count"+"_"+value, "0");
                    count = Integer.toString(Integer.parseInt(count) - 1);
                }
                synchronized(globalHashMap){
                    globalHashMap.put("count"+"_"+value, count);
                    return globalHashMap.remove(key);
                }
            } else {
                Stack stack = new Stack();
                LocalMemcache latestLocalMemcache = localMemcache;
                while(latestLocalMemcache != null) {
                    stack.push(latestLocalMemcache);
                    latestLocalMemcache = latestLocalMemcache.localMemcache;
                }
                
                LocalMemcache top = (LocalMemcache) stack.pop();
                LocalMemcache topMost = top;
                while (top != null) {
                    value = top.localHashMap.get(key);
                    if (value != null) {
                        value = top.localHashMap.get(key);
                        String count  = top.localHashMap.get("count"+"_"+value);
                        top.localHashMap.put("count"+"_"+value, Integer.toString(Integer.parseInt(count) - 1));
                        topMost.deletedKeys.put(key, value);

                        return top.localHashMap.remove(key);
                    } else {
                        if(!stack.empty()) {
                            top = (LocalMemcache) stack.pop();
                        } else {
                            break;
                        }
                        
                    }
                }
                synchronized(globalHashMap) {
                    return globalHashMap.remove(key);
                }
                
                
            }
        }
    }
    
    private int count(String key) {
        int globalCount = 0;
        while(true) {
            if (localMemcache == null) {
                return Integer.parseInt(globalHashMap.getOrDefault("count_"+key, "0"));
            } else {
                Stack stack = new Stack();
                LocalMemcache latestLocalMemcache = localMemcache;
                while(latestLocalMemcache != null) {
                    stack.push(latestLocalMemcache);
                    latestLocalMemcache = latestLocalMemcache.localMemcache;
                }
                
                LocalMemcache top = (LocalMemcache) stack.pop();
                while (top != null) {
                    globalCount += Integer.parseInt(top.localHashMap.getOrDefault("count_"+key, "0"));
                    
                    if (stack.isEmpty()) {
                        break;
                    }
                    top = (LocalMemcache) stack.pop();
                }
                
                return globalCount + Integer.parseInt(globalHashMap.getOrDefault("count_"+key, "0"));
                
            }
        }
    }
    
    private void commit() {
        HashMap<String, String> previousHashMap ;
        LocalMemcache latestLocalMemcache = localMemcache;
        LocalMemcache previousLocalMemcache = null;
        
        if (latestLocalMemcache == null) {
            System.out.println("Do start first");
        } else {
            while(latestLocalMemcache.localMemcache != null) {
                previousHashMap = latestLocalMemcache.localHashMap;
                previousLocalMemcache = latestLocalMemcache;
                latestLocalMemcache = latestLocalMemcache.localMemcache;
            }
            if (previousLocalMemcache == null) {
                synchronized(this){
                    globalHashMap = merge(globalHashMap, latestLocalMemcache);
                }
                localMemcache = null;
            } else {
                previousLocalMemcache.localHashMap = merge(previousLocalMemcache.localHashMap, latestLocalMemcache);
                previousLocalMemcache.localMemcache = null;
            }
        }
    }
    
    private void start() {
        if(localMemcache == null) {
            localMemcache = new LocalMemcache();
            return ;
        }
        LocalMemcache latestLocalMemcache = findLatestLocalMemcache();
        if (latestLocalMemcache == null) {
            localMemcache = new LocalMemcache();
        } else {
            latestLocalMemcache.localMemcache = new LocalMemcache();
        }

        latestLocalMemcache = localMemcache;
        int  varcount= 0;
        while(latestLocalMemcache != null) {
            latestLocalMemcache = latestLocalMemcache.localMemcache;
        }
    }
    
    private LocalMemcache findLatestLocalMemcache() {
        if (localMemcache == null) {
            return null;
        } else {
            LocalMemcache latestLocalMemcache = localMemcache;
            while(latestLocalMemcache.localMemcache != null) {
                latestLocalMemcache = latestLocalMemcache.localMemcache;

            }
            return latestLocalMemcache;
        }
    }
    
    private void rollback() {
        if (localMemcache == null) {
            System.out.println("No rollback without start");

            return ;
        }
        Stack stack = new Stack();
        LocalMemcache latestLocalMemcache = localMemcache;
        while(latestLocalMemcache != null) {
            stack.push(latestLocalMemcache);
            latestLocalMemcache = latestLocalMemcache.localMemcache;

        }

        LocalMemcache top = (LocalMemcache) stack.pop();
        HashMap<String, String> deletedKeys;
        
        if (stack.empty()) {
            deletedKeys = localMemcache.deletedKeys;

            localMemcache = null;
        } else {
            LocalMemcache PreviousTop = (LocalMemcache) stack.pop();
            deletedKeys = PreviousTop.localMemcache.deletedKeys;
            PreviousTop.localMemcache = null;
        }
        
        //Adding back deleted ones
        if (deletedKeys != null) {
            for (Map.Entry entry : deletedKeys.entrySet()) {
                String key = (String) entry.getKey();
                String value = (String) entry.getValue();
                set(key, value);
            }  
        }
    }
    
    private HashMap<String, String> merge(HashMap<String, String> parentHashMap, LocalMemcache childMemcache) {
        for ( String key : childMemcache.deletedKeys.keySet() ) {
            parentHashMap.remove(key);
        }

        List<String> keys = new ArrayList<>(parentHashMap.keySet());
        keys.addAll(childMemcache.localHashMap.keySet());
        
        HashMap<String, String> mergedHashMap = new HashMap<>();
        for (String k : keys) {
          if (k.startsWith("count_")) {
              mergedHashMap.put(k, Integer.toString(Integer.parseInt(parentHashMap.getOrDefault(k, "0")) + Integer.parseInt(childMemcache.localHashMap.getOrDefault(k, "0"))));
          } else {
              String value = childMemcache.localHashMap.get(k);
              if (value == null) {
                  value = parentHashMap.get(k);
              }
              mergedHashMap.put(k, value);
          }
        }
        
        return mergedHashMap;
    }
}