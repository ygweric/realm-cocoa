////////////////////////////////////////////////////////////////////////////
//
// Copyright 2014 Realm Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//
////////////////////////////////////////////////////////////////////////////

#import "AppDelegate.h"
#import <Realm/Realm.h>

#define VERSION 1

@interface Person : RLMObject
#if VERSION == 0
@property NSString *name;
#elif VERSION == 1
@property NSString *fullName;
#endif
@end
@implementation Person
@end

@implementation AppDelegate

- (BOOL)application:(UIApplication *)application didFinishLaunchingWithOptions:(NSDictionary *)launchOptions
{
    self.window = [[UIWindow alloc] initWithFrame:[[UIScreen mainScreen] bounds]];
    self.window.rootViewController = [[UIViewController alloc] init];
    [self.window makeKeyAndVisible];
    NSLog(@"%@", [RLMRealmConfiguration defaultConfiguration].path);

#if VERSION == 0
    [[NSFileManager defaultManager] removeItemAtPath:[RLMRealmConfiguration defaultConfiguration].path error:nil];

    RLMRealm *realm = [RLMRealm defaultRealm];
    [realm transactionWithBlock:^{
        [Person createInRealm:realm withValue:@[@"John"]];
    }];
#elif VERSION == 1
    RLMRealmConfiguration *config = [RLMRealmConfiguration defaultConfiguration];
    config.schemaVersion = 1;

    config.migrationBlock = ^(RLMMigration *migration, uint64_t oldSchemaVersion){
        [migration renameClassName:@"Person" property:@"name" newProperty:@"fullName"];
    };
    [RLMRealmConfiguration setDefaultConfiguration:config];
#endif

    NSLog(@"%@", [Person allObjects]);
    return YES;
}

@end
