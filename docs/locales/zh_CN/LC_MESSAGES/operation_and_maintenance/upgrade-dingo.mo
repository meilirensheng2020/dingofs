Þ                      h  }   i  5   ç  >     [   \  #   ¸  #   Ü  %      À   &     ç  5   ÷  4   -  6   b         ¬  O  ¿  x     /     >   ¸  <   ÷     4     S     r          .	  ,   ;	  "   h	  ,   	  Ó   ¸	     
              	                                          
                        DingoAdm upgrades all services in the cluster by default. To upgrade a specific service, you can add the following 3 options: Example 1: Upgrading a service with id `c9570c0d0252` Example 2: Upgrading all `MDS` services on the host `10.0.1.1` Modify the mirror names in the cluster topology to the names of the mirrors to be upgraded: Step 1: Modify the cluster topology Step 2: Commit the cluster topology Step 3: Upgrade the specified service The above 3 options can be used in any combination, and the corresponding `id`, `host`, and `role` of the services can be viewed via [dingoadm status](./maintain-dingo#viewing-cluster-status). Upgrade Service `--host`: Upgrade all services on the specified host. `--id`: Upgrade the service with the specified `id`. `--role`: Upgrade all services for the specified role. `upgrade` upgrades each service specified on a rolling basis by default. Users  need to go into the container to determine if the cluster is healthy after upgrading each service. If you want to perform the upgrade in one go, you can add the `-f` option to force the upgrade. ð¡ **REMINDER:** Project-Id-Version: DingoFS
Report-Msgid-Bugs-To: 
PO-Revision-Date: 2025-06-05 18:35+0800
Last-Translator: 
Language-Team: zh_CN <LL@li.org>
Language: zh_CN
MIME-Version: 1.0
Content-Type: text/plain; charset=utf-8
Content-Transfer-Encoding: 8bit
Plural-Forms: nplurals=1; plural=0;
Generated-By: Babel 2.17.0
X-Generator: Poedit 3.6
 DingoAdm é»è®¤åçº§éç¾¤ä¸­çæææå¡ï¼å¦éåçº§æå®æå¡ï¼å¯éè¿æ·»å ä»¥ä¸ 3 ä¸ªéé¡¹æ¥å®ç°ï¼ ç¤ºä¾ 1ï¼åçº§ id ä¸º c9570c0d0252 çæå¡ ç¤ºä¾ 2ï¼åçº§ 10.0.1.1 è¿å°ä¸»æºä¸çææ MDS æå¡ ä¿®æ¹éç¾¤ææä¸­çéååä¸ºéåçº§çéååï¼ ç¬¬ 1 æ­¥ï¼ä¿®æ¹éç¾¤ææ ç¬¬ 2 æ­¥ï¼æäº¤éç¾¤ææ ç¬¬ 3 æ­¥ï¼åçº§æå®æå¡ ä»¥ä¸ 3 ä¸ªéé¡¹å¯ä»»æç»åä½¿ç¨ï¼æå¡å¯¹åºç idãhostãrole å¯éè¿ [dingoadm status](./maintain-dingo#viewing-cluster-status) æ¥æ¥çã åçº§æå¡ --host: åçº§æå®ä¸»æºçæææå¡ã --id: åçº§æå® id çæå¡ã --role: åçº§æå®è§è²çæææå¡ã upgrade é»è®¤ä¼æ»å¨åçº§æå®çæ¯ä¸ä¸ªæå¡ï¼ç¨æ·å¨åçº§æ¯ä¸ä¸ªæå¡å éè¿å¥å®¹å¨åç¡®å®éç¾¤æ¯å¦å¥åº·ãè¥ç¨æ·æ³ä¸æ¬¡æ§æ§è¡åçº§æä½ï¼å¯æ·»å  -f éé¡¹å¼ºå¶åçº§ã ð¡ **æé** 